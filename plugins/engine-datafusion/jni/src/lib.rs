/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::{Array, StructArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use jni::objects::{JByteArray, JClass, JObject};
use jni::sys::{jbyteArray, jint, jlong, jstring};
use jni::{JNIEnv, JavaVM};
use std::ptr::addr_of_mut;
use std::sync::{Arc, OnceLock};
use std::sync::Once;

mod util;
mod row_id_optimizer;
mod listing_table;
mod runtime_manager;
mod cross_rt_stream;
mod executor;
mod io;

use datafusion::execution::context::SessionContext;
use tokio_metrics::{RuntimeMonitor, TaskMonitor};
use log::{info, error};
use crate::listing_table::{ListingOptions, ListingTable, ListingTableConfig};
use crate::util::{create_object_meta_from_filenames, parse_string_arr, set_object_result_error, set_object_result_ok, set_object_result_error_global, set_object_result_ok_global};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::cache_unit::DefaultListFilesCache;
use datafusion::execution::cache::CacheAccessor;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::SessionConfig;
use datafusion::DATAFUSION_VERSION;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use futures::TryStreamExt;
use jni::objects::{JObjectArray, JString};
use object_store::ObjectMeta;
use prost::Message;
use std::time::Duration;
use std::sync::atomic::{AtomicU64, Ordering};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use once_cell::sync::Lazy;
use crate::cross_rt_stream::CrossRtStream;
use crate::runtime_manager::RuntimeManager;

use crate::executor::DedicatedExecutor;
use std::cell::RefCell;
use datafusion::execution::RecordBatchStream;

static QUERY_EXECUTION_MONITOR: Lazy<TaskMonitor> = Lazy::new(|| {
    TaskMonitor::with_slow_poll_threshold(Duration::from_micros(100)).clone()
});

static STREAM_NEXT_MONITOR: Lazy<TaskMonitor> = Lazy::new(|| {
    TaskMonitor::with_slow_poll_threshold(Duration::from_micros(50)).clone()
});

// Global runtime manager
static RUNTIME_MANAGER: OnceLock<Arc<RuntimeManager>> = OnceLock::new();
/// Counter for periodic metrics logging
static STREAM_CALL_COUNTER: AtomicU64 = AtomicU64::new(0);

// Global JavaVM reference
static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();

thread_local! {
    static THREAD_JNIENV: RefCell<Option<JNIEnv<'static>>> = RefCell::new(None);
}

// Helper function to get or attach JNI env
fn with_jni_env<F, R>(f: F) -> R
where
    F: FnOnce(&mut JNIEnv) -> R,
{
    THREAD_JNIENV.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() {
            let jvm = JAVA_VM.get().expect("JavaVM not initialized");
            let env = jvm.attach_current_thread_permanently()
                .expect("Failed to attach thread to JVM");
            *opt = Some(env);
        }

        // Safe because we're the only one with access to this thread-local
        let env_ref = opt.as_mut().unwrap();
        f(env_ref)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_initRuntimeManager(
    env: JNIEnv,
    _class: JClass,
    cpu_threads: jint,
) {
    // Initialize JavaVM once
    JAVA_VM.get_or_init(|| {
        env.get_java_vm().expect("Failed to get JavaVM")
    });
     // Initialize tokio console with custom configuration
    // console_subscriber::ConsoleLayer::builder()
    //     .with_default_env()
    //     .init();
    //
    //  match console_subscriber::ConsoleLayer::builder()
    //         .server_addr(([0, 0, 0, 0], 6669)) // Listen on all interfaces
    //         .retention(std::time::Duration::from_secs(60))
    //         .spawn()
    //     {
    //         Ok(_) => {
    //             println!("✓ Console subscriber initialized successfully on port 6669");
    //             println!("  Run: tokio-console http://127.0.0.1:6669");
    //         }
    //         Err(e) => {
    //             eprintln!("✗ Failed to initialize console subscriber: {}", e);
    //         }
    //     }

    RUNTIME_MANAGER.get_or_init(|| {
        println!("Runtime manager initialized with {} CPU threads", cpu_threads);
        Arc::new(RuntimeManager::new(cpu_threads as usize))
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_shutdownRuntimeManager(
    _env: JNIEnv,
    _class: JClass,
) {
    if let Some(_mgr) = RUNTIME_MANAGER.get() {
        // Runtimes will be dropped and shut down when RUNTIME_MANAGER is dropped
        info!("Runtime manager shut down");
    }
}

/// Create a new DataFusion session context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createContext(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config(config);
    Box::into_raw(Box::new(context)) as jlong
}

/// Close and cleanup a DataFusion context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    if context_id != 0 {
        let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
    }
}

/// Get version information
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_getVersionInfo(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let version_info = format!(r#"{{"version": "{}", "codecs": ["CsvDataSourceCodec"]}}"#, DATAFUSION_VERSION);
    env.new_string(version_info).expect("Couldn't create Java string").as_raw()
}

/// Get version information (legacy method name)
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_getVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    env.new_string(DATAFUSION_VERSION).expect("Couldn't create Java string").as_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createTokioRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    // TODO : remove this
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let ctx = Box::into_raw(Box::new(rt)) as jlong;
    ctx
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_startRuntimeMonitoring(
    _env: JNIEnv,
    _class: JClass,
) {
    let manager = match RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            error!("Runtime manager not initialized");
            return;
        }
    };

    let io_runtime = manager.io_runtime.clone();
     io_runtime.spawn(async move {
        let handle = tokio::runtime::Handle::current();
        let runtime_monitor = RuntimeMonitor::new(&handle);

        // Monitor at 120-second intervals
        for metrics in runtime_monitor.intervals() {
            log_runtime_metrics(&metrics);
            tokio::time::sleep(Duration::from_secs(120)).await;
        }
    });
    //
    // println!("Runtime monitoring started");
}

/// Log runtime metrics with performance analysis
fn log_runtime_metrics(metrics: &tokio_metrics::RuntimeMetrics) {
    println!("=== Runtime Metrics ===");
    println!("  Workers: {}", metrics.workers_count);
    println!("  Global queue depth: {}", metrics.global_queue_depth);
    println!("  Worker overflow: {}", metrics.total_overflow_count);
    println!("  Remote schedule: {}", metrics.max_local_schedule_count);
    println!("  Worker steal ops: {}", metrics.total_steal_operations);
    println!("  Blocking queue depth: {}", metrics.blocking_queue_depth);

    let query_metrics = QUERY_EXECUTION_MONITOR.cumulative();
    log_task_metrics("Query exec (via CrossRtStream)", &query_metrics);
    let stream_metrics = STREAM_NEXT_MONITOR.cumulative();
    log_task_metrics("Stream Next (via CrossRtStream)", &stream_metrics);
    println!("======================");
}

/// Log task metrics with performance analysis
fn log_task_metrics(operation: &str, metrics: &tokio_metrics::TaskMetrics) {
    println!("=== Task Metrics: {} ===", operation);
    println!("  Scheduled duration: {:?}", metrics.total_scheduled_duration);
    println!("  Poll duration: {:?}", metrics.total_poll_duration);
    println!("  Idle duration: {:?}", metrics.total_idle_duration);
    println!("  Mean poll duration: {:?}", metrics.mean_poll_duration());
    println!("  Slow poll ratio: {:.2}%", metrics.slow_poll_ratio() * 100.0);
    println!("  Mean first poll delay: {:?}", metrics.mean_first_poll_delay());
    println!("  Total slow polls: {}", metrics.total_slow_poll_count);
    println!("  Total long delays: {}", metrics.total_long_delay_count);
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let runtime_env = RuntimeEnvBuilder::default().build().unwrap();
    Box::into_raw(Box::new(runtime_env)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
    runtime_id: jlong,
) -> jlong {
    if runtime_id == 0 {
        return 0;
    }
    let runtime_env = unsafe { Arc::from_raw(runtime_id as *const RuntimeEnv) };
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config_rt(config, runtime_env.clone());
    let _ = Arc::into_raw(runtime_env);

    Box::into_raw(Box::new(context)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeSessionContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    if context_id != 0 {
        let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    files: JObjectArray
) -> jlong {
    let table_path: String = env.get_string(&table_path).expect("Couldn't get java string!").into();
    let files: Vec<String> = parse_string_arr(&mut env, files).expect("Expected list of files");
    let files_meta = create_object_meta_from_filenames(&table_path, files);
    let table_path = ListingTableUrl::parse(table_path).unwrap();
    let shard_view = ShardView::new(table_path, files_meta);
    Box::into_raw(Box::new(shard_view)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_destroyReader(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong
) {
    if ptr != 0 {
        let _ = unsafe { Box::from_raw(ptr as *mut ShardView) };
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_freeStream(
    _env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong,
) {
    if stream_ptr != 0 {
        unsafe {
            let _ = Box::from_raw(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>);
        }
    }
}

pub struct ShardView {
    table_path: ListingTableUrl,
    files_meta: Arc<Vec<ObjectMeta>>
}

impl ShardView {
    pub fn new(table_path: ListingTableUrl, files_meta: Vec<ObjectMeta>) -> Self {
        let files_meta = Arc::new(files_meta);
        ShardView {
            table_path,
            files_meta
        }
    }
    pub fn table_path(&self) -> ListingTableUrl {
        self.table_path.clone()
    }
    pub fn files_meta(&self) -> Arc<Vec<ObjectMeta>> {
        self.files_meta.clone()
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_executeSubstraitQuery(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    substrait_bytes: jbyteArray,
) -> jlong {

    let manager = match RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            error!("Runtime manager not initialized");
            return 0;
        }
    };

    let io_runtime = manager.io_runtime.clone();
    let cpu_executor = manager.cpu_executor();
    // spawn on io runtime executor
    io_runtime.block_on(async move {
        let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
        let table_name: String = env.get_string(&table_name)
            .expect("Couldn't get java string!").into();

        let table_path = shard_view.table_path();
        let files_meta = shard_view.files_meta();

        let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
        let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to convert plan bytes: {}", e);
                return 0;
            }
        };
        //QUERY_EXECUTION_MONITOR.instrument(async move {
            let list_file_cache = Arc::new(DefaultListFilesCache::default());
            list_file_cache.put(table_path.prefix(), files_meta);

            let runtime_env = match RuntimeEnvBuilder::new()
                .with_cache_manager(CacheManagerConfig::default()
                    .with_list_files_cache(Some(list_file_cache.clone()))
                ).build() {
                Ok(env) => env,
                Err(e) => {
                    error!("Failed to build runtime env: {}", e);
                    return 0;
                }
            };

            let mut config = SessionConfig::new();
            config.options_mut().execution.parquet.pushdown_filters = false;
            config.options_mut().execution.target_partitions = 1;

            let state = datafusion::execution::SessionStateBuilder::new()
                .with_config(config)
                .with_runtime_env(Arc::new(runtime_env))
                .with_default_features()
                .build();

            let ctx = SessionContext::new_with_state(state);

            // Register table
            let file_format = ParquetFormat::new();
            let listing_options = ListingOptions::new(Arc::new(file_format))
                .with_file_extension(".parquet");

            let resolved_schema = match listing_options
                .infer_schema(&ctx.state(), &table_path)
                .await {
                Ok(schema) => schema,
                Err(e) => {
                    error!("Failed to infer schema: {}", e);
                    return 0;
                }
            };

            let table_config = ListingTableConfig::new(table_path.clone())
                .with_listing_options(listing_options)
                .with_schema(resolved_schema);

            let provider = match ListingTable::try_new(table_config) {
                Ok(table) => Arc::new(table),
                Err(e) => {
                    error!("Failed to create listing table: {}", e);
                    return 0;
                }
            };

            if let Err(e) = ctx.register_table(&table_name, provider) {
                error!("Failed to register table: {}", e);
                return 0;
            }

            // Decode substrait
            let substrait_plan = match Plan::decode(plan_bytes_vec.as_slice()) {
                Ok(plan) => {
                    //info!("Substrait plan decoded successfully");
                    plan
                },
                Err(e) => {
                    error!("Failed to decode Substrait plan: {}", e);
                    return 0;
                }
            };

            let logical_plan = match from_substrait_plan(&ctx.state(), &substrait_plan).await {
                Ok(plan) => plan,
                Err(e) => {
                    error!("Failed to convert Substrait plan: {}", e);
                    return 0;
                }
            };

            // let df_stream = cpu_executor.spawn(async move {
            //     let dataframe = ctx.execute_logical_plan(logical_plan).await?;
            //     dataframe.execute_stream().await
            // }).await
            // .map_err(|e| DataFusionError::Execution(format!("Executor error: {:?}", e)))
            // .map_err(|e| DataFusionError::Execution(format!("Stream error: {}", e)));

            let dataframe = match ctx.execute_logical_plan(logical_plan).await {
                Ok(df) => df,
                Err(e) => {
                    error!("Failed to execute logical plan: {}", e);
                    return 0;
                }
            };

            let df_stream = match dataframe.execute_stream().await {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to create execution stream: {}", e);
                    return 0;
                }
            };


        // CrossRtStream transfer cpu heavy tasks to CPU executor
            let cross_rt_stream = CrossRtStream::new_with_df_error_stream(
                df_stream,
                cpu_executor,
            );

            let wrapped_stream = RecordBatchStreamAdapter::new(
                cross_rt_stream.schema(),
                cross_rt_stream,
            );
            // let call_count = STREAM_CALL_COUNTER.fetch_add(1, Ordering::Relaxed);
            // if call_count % 200 == 1 {
            //     let metrics = QUERY_EXECUTION_MONITOR.cumulative();
            //     log_task_metrics("Query exec (via CrossRtStream)", &metrics);
            // }
            Box::into_raw(Box::new(wrapped_stream)) as jlong
       // }).await
    })
}


/// ASYNC VERSION - Preferred method for query execution
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_executeSubstraitQueryAsync(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    substrait_bytes: jbyteArray,
    callback: JObject,
) {
    let manager = match RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            error!("Runtime manager not initialized");
            set_object_result_error(&mut env, callback,
                                    &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return;
        }
    };

    // Convert callback to GlobalRef (thread-safe)
    let callback_ref = match env.new_global_ref(&callback) {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to create global ref: {}", e);
            set_object_result_error(&mut env, callback,
                                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };

    // Extract data before spawning
    let table_name: String = match env.get_string(&table_name) {
        Ok(s) => s.into(),
        Err(e) => {
            error!("Failed to get table name: {}", e);
            set_object_result_error(&mut env, callback,
                                    &DataFusionError::Execution(format!("Failed to get table name: {}", e)));
            return;
        }
    };

    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("Failed to convert plan bytes: {}", e);
            set_object_result_error(&mut env, callback,
                                    &DataFusionError::Execution(format!("Failed to convert plan bytes: {}", e)));
            return;
        }
    };

    let io_runtime = manager.io_runtime.clone();
    let cpu_executor = manager.cpu_executor();

    // Spawn async task - TRULY NON-BLOCKING!
    io_runtime.spawn(async move {
        let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
        let table_path = shard_view.table_path();
        let files_meta = shard_view.files_meta();

        // Execute query with monitoring
        let result = QUERY_EXECUTION_MONITOR.instrument(
            execute_query_internal(
                table_path,
                files_meta,
                table_name,
                plan_bytes_vec,
                cpu_executor,
            )
        ).await;

        // let result = execute_query_internal(
        //     table_path,
        //     files_meta,
        //     table_name,
        //     plan_bytes_vec,
        //     cpu_executor,
        // ).await;

        match result {
            Ok(stream_ptr) => {
                // Use thread-local JNI env - auto-attaches!
                with_jni_env(|env| {
                    //println!("cross rt ptr : {}", stream_ptr);
                    set_object_result_ok_global(env, &callback_ref, stream_ptr as *mut u8);
                });
            }
            Err(e) => {
                // Use thread-local JNI env - auto-attaches!
                with_jni_env(|env| {
                    error!("Query execution failed: {}", e);
                    set_object_result_error_global(env, &callback_ref, &e);
                });
            }
        }
    });

    // Function returns immediately - async work continues in background
}

// Extract query execution logic
async fn execute_query_internal(
    table_path: ListingTableUrl,
    files_meta: Arc<Vec<ObjectMeta>>,
    table_name: String,
    plan_bytes_vec: Vec<u8>,
    cpu_executor: DedicatedExecutor,
) -> Result<jlong, DataFusionError> {
    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(table_path.prefix(), files_meta);

    let runtime_env = match RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default()
            .with_list_files_cache(Some(list_file_cache.clone()))
        )
        .build() {
        Ok(env) => env,
        Err(e) => {
            error!("Failed to build runtime env: {}", e);
            return Err(e);
        }
    };

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = false;
    config.options_mut().execution.target_partitions = 1; // TODO : this can be more than 1 for higher instance types ?

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::new(runtime_env))
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Register table
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet");

    let resolved_schema = match listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await {
        Ok(schema) => schema,
        Err(e) => {
            error!("Failed to infer schema: {}", e);
            return Err(e);
        }
    };

    let table_config = ListingTableConfig::new(table_path.clone())
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    let provider = match ListingTable::try_new(table_config) {
        Ok(table) => Arc::new(table),
        Err(e) => {
            error!("Failed to create listing table: {}", e);
            return Err(e);
        }
    };

    if let Err(e) = ctx.register_table(&table_name, provider) {
        error!("Failed to register table: {}", e);
        return Err(e);
    }

    // Decode substrait
    let substrait_plan = match Plan::decode(plan_bytes_vec.as_slice()) {
        Ok(plan) => plan,
        Err(e) => {
            error!("Failed to decode Substrait plan: {}", e);
            return Err(DataFusionError::Execution(format!("Failed to decode Substrait: {}", e)));
        }
    };

    let logical_plan = match from_substrait_plan(&ctx.state(), &substrait_plan).await {
        Ok(plan) => plan,
        Err(e) => {
            error!("Failed to convert Substrait plan: {}", e);
            return Err(e);
        }
    };

    let dataframe = match ctx.execute_logical_plan(logical_plan).await {
        Ok(df) => df,
        Err(e) => {
            error!("Failed to execute logical plan: {}", e);
            return Err(e);
        }
    };

    let df_stream = match dataframe.execute_stream().await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to create execution stream: {}", e);
            return Err(e);
        }
    };

    // CrossRtStream transfers CPU heavy tasks to CPU executor
    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(
        df_stream,
        cpu_executor,
    );

    let wrapped_stream = RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );

    Ok(Box::into_raw(Box::new(wrapped_stream)) as jlong)
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_next(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    stream: jlong,
    callback: JObject,
) {
    let manager = match RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            set_object_result_error(
                &mut env,
                callback,
                &DataFusionError::Execution("Runtime manager not initialized".to_string())
            );
            return;
        }
    };

    // Convert callback to GlobalRef
    let callback_ref = match env.new_global_ref(&callback) {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to create global ref: {}", e);
            set_object_result_error(&mut env, callback,
                                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };

    let stream_ptr = stream;
    let io_runtime = manager.io_runtime.clone();

    io_runtime.spawn(async move {
        let stream = unsafe { &mut *(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };

        // Poll the stream with monitoring
        //let result = stream.try_next().await;

        let result = STREAM_NEXT_MONITOR.instrument(async {
                stream.try_next().await
        }).await;

        // Use thread-local JNI env - auto-attaches!
        with_jni_env(|env| {
            match result {
                Ok(Some(batch)) => {
                    // Convert to FFI
                    let struct_array: StructArray = batch.into();
                    let array_data = struct_array.into_data();
                    let ffi_array = FFI_ArrowArray::new(&array_data);
                    let ffi_array_ptr = Box::into_raw(Box::new(ffi_array));
                    set_object_result_ok_global(env, &callback_ref, ffi_array_ptr);
                }
                Ok(None) => {
                    // End of stream
                    set_object_result_ok_global(env, &callback_ref, std::ptr::null_mut::<FFI_ArrowSchema>());
                }
                Err(err) => {
                    error!("Stream next failed: {}", err);
                    set_object_result_error_global(env, &callback_ref, &err);
                }
            }
        });
    });

    // Function returns immediately - async work continues in background
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_getSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong,
    callback: JObject,
) {
    if stream_ptr == 0 {
        set_object_result_error(
            &mut env,
            callback,
            &DataFusionError::Execution("Invalid stream pointer".to_string())
        );
        return;
    }
    // Schema access is synchronous and fast - no need for runtime
    let stream = unsafe { &mut *(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };
    let schema = stream.schema();
    match FFI_ArrowSchema::try_from(schema.as_ref()) {
        Ok(mut ffi_schema) => {
            set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_schema));
        }
        Err(err) => {
            set_object_result_error(&mut env, callback, &DataFusionError::Execution(
                format!("Schema conversion failed: {}", err)
            ));
        }
    }
}

/// Export current metrics as JSON for Java consumption
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_getMetricsJson(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let query_metrics = QUERY_EXECUTION_MONITOR.cumulative();
    let stream_metrics = STREAM_NEXT_MONITOR.cumulative();

    let metrics_json = format!(
        r#"{{
            "query_execution": {{
                "scheduled_duration_ms": {},
                "poll_duration_ms": {},
                "idle_duration_ms": {},
                "mean_poll_us": {},
                "slow_poll_ratio": {},
                "mean_first_poll_delay_us": {},
                "slow_poll_count": {},
                "long_delay_count": {}
            }},
            "stream_operations": {{
                "scheduled_duration_ms": {},
                "poll_duration_ms": {},
                "slow_poll_ratio": {}
            }}
        }}"#,
        query_metrics.total_scheduled_duration.as_millis(),
        query_metrics.total_poll_duration.as_millis(),
        query_metrics.total_idle_duration.as_millis(),
        query_metrics.mean_poll_duration().as_micros(),
        query_metrics.slow_poll_ratio(),
        query_metrics.mean_first_poll_delay().as_micros(),
        query_metrics.total_slow_poll_count,
        query_metrics.total_long_delay_count,
        stream_metrics.total_scheduled_duration.as_millis(),
        stream_metrics.total_poll_duration.as_millis(),
        stream_metrics.slow_poll_ratio(),
    );

    env.new_string(metrics_json)
        .expect("Couldn't create Java string")
        .as_raw()
}

/// Reset all metrics counters
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_resetMetrics(
    _env: JNIEnv,
    _class: JClass,
) {
    STREAM_CALL_COUNTER.store(0, Ordering::Relaxed);
    info!("Metrics counters reset");
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_closeStream(
    _env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong
) {
    if stream_ptr != 0 {
        let _ = unsafe { Box::from_raw(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
    runtime: jlong
) {
    if runtime != 0 {
        let _ = unsafe { Box::from_raw(runtime as *mut RuntimeEnv) };
    }
}
