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
use jni::JNIEnv;
use std::ptr::addr_of_mut;
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::time::Instant;


/// Initialize the logger once
use simple_logger::SimpleLogger;
mod util;
mod row_id_optimizer;
mod listing_table;
mod runtime_manager;
mod cross_rt_stream;
mod executor;
mod io;

use datafusion::execution::context::SessionContext;
use tokio_metrics::{RuntimeMonitor, TaskMonitor};
use log::{info, error, warn};
use crate::listing_table::{ListingOptions, ListingTable, ListingTableConfig};
use crate::util::{create_object_meta_from_filenames, parse_string_arr, set_object_result_error, set_object_result_ok};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::cache_unit::DefaultListFilesCache;
use datafusion::execution::cache::CacheAccessor;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{ParquetReadOptions, SessionConfig};
use datafusion::DATAFUSION_VERSION;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use jni::objects::{JObjectArray, JString};
use object_store::ObjectMeta;
use prost::Message;
use tokio::runtime::Runtime;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use once_cell::sync::Lazy;
use crate::cross_rt_stream::CrossRtStream;
use crate::runtime_manager::RuntimeManager;

use std::sync::mpsc as std_mpsc;
use crate::executor::DedicatedExecutor;

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

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_initRuntimeManager(
    _env: JNIEnv,
    _class: JClass,
    cpu_threads: jint,
) {

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
    let mut manager = RUNTIME_MANAGER.get();
    if let Some(mgr) = manager.take() {
        // Runtimes will be dropped and shut down
        drop(mgr);
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
    let ctx = Box::into_raw(Box::new(context)) as jlong;
    ctx
}
/// Close and cleanup a DataFusion context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
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
    tokio_runtime_ptr: jlong,
) {
    let runtime = unsafe { &*(tokio_runtime_ptr as *const Runtime) };
    let handle = runtime.handle().clone();

    runtime.spawn(async move {
        let runtime_monitor = RuntimeMonitor::new(&handle);

        // Monitor at 5-second intervals
        for metrics in runtime_monitor.intervals() {
            log_runtime_metrics(&metrics);
            tokio::time::sleep(Duration::from_secs(120)).await;
        }
    });

    info!("Runtime monitoring started");
}

/// Log runtime metrics with performance analysis
fn log_runtime_metrics(metrics: &tokio_metrics::RuntimeMetrics) {
    info!("=== Runtime Metrics ===");
    info!("  Workers: {}", metrics.workers_count);
    info!("  Global queue depth: {}", metrics.global_queue_depth);
    info!("  Worker overflow: {}", metrics.total_overflow_count);
    info!("  Remote schedule: {}", metrics.max_local_schedule_count);
    info!("  Worker steal ops: {}", metrics.total_steal_operations);
    info!("  Blocking queue depth: {}", metrics.blocking_queue_depth);
//     //
    // // Performance guide recommendations: Check for runtime-level issues
    //
    // // High global queue depth indicates external thread chatter or overflow
    // if metrics.global_queue_depth > 50 {
    //     warn!("HIGH GLOBAL QUEUE DEPTH: {} - indicates external thread chatter or worker overflow",
    //           metrics.global_queue_depth);
    // }
    //
    // // Worker overflow indicates uneven load or too many small tasks
    // if metrics.max_steal_operations > 20 {
    //     warn!("HIGH WORKER OVERFLOW: {} - consider consolidating tasks or balancing load",
    //           metrics.max_steal_operations);
    // }
    //
    // // Remote schedule count indicates external thread interaction
    // if metrics.max_local_schedule_count > 50 {
    //     warn!("HIGH REMOTE SCHEDULE COUNT: {} - external threads spawning/waking tasks frequently",
    //           metrics.max_local_schedule_count);
    // }
    //
    // // High blocking queue indicates spawn_blocking contention
    // if metrics.blocking_queue_depth > 10 {
    //     warn!("HIGH BLOCKING QUEUE DEPTH: {} - spawn_blocking contention detected",
    //           metrics.blocking_queue_depth);
    // }
    let metrics = QUERY_EXECUTION_MONITOR.cumulative();
    log_task_metrics("Query exec (via CrossRtStream)", &metrics);
    let metrics = STREAM_NEXT_MONITOR.cumulative();
    log_task_metrics("Stream Next (via CrossRtStream)", &metrics);
    info!("======================");
}

/// Log task metrics with performance analysis
fn log_task_metrics(operation: &str, metrics: &tokio_metrics::TaskMetrics) {
    info!("=== Task Metrics: {} ===", operation);
    info!("  Scheduled duration: {:?}", metrics.total_scheduled_duration);
    info!("  Poll duration: {:?}", metrics.total_poll_duration);
    info!("  Idle duration: {:?}", metrics.total_idle_duration);
    info!("  Mean poll duration: {:?}", metrics.mean_poll_duration());
    info!("  Slow poll ratio: {:.2}%", metrics.slow_poll_ratio() * 100.0);
    info!("  Mean first poll delay: {:?}", metrics.mean_first_poll_delay());
    info!("  Total slow polls: {}", metrics.total_slow_poll_count);
    info!("  Total long delays: {}", metrics.total_long_delay_count);
//
//     // Performance guide recommendations: Check for issues
//
//     // High scheduling delay indicates Tokio scheduling issues
//     if metrics.total_scheduled_duration > Duration::from_millis(10) {
//         warn!("HIGH SCHEDULING DELAY for {}: {:?} - investigate global queue or long polls",
//               operation, metrics.total_scheduled_duration);
//     }
//
//     // High slow poll ratio indicates blocking work in async context
//     if metrics.slow_poll_ratio() > 0.1 {
//         warn!("HIGH SLOW POLL RATIO for {}: {:.2}% - consider using spawn_blocking or yield_now",
//               operation, metrics.slow_poll_ratio() * 100.0);
//     }
//
//     // High first poll delay indicates external thread chatter or global queue issues
//     if metrics.mean_first_poll_delay() > Duration::from_millis(5) {
//         warn!("HIGH FIRST POLL DELAY for {}: {:?} - likely external thread chatter or global queue issues",
//               operation, metrics.mean_first_poll_delay());
//     }
//
//     // High long delay count indicates scheduling delays
//     if metrics.total_long_delay_count > 10 {
//         warn!("HIGH LONG DELAY COUNT for {}: {} - tasks waiting too long to be polled",
//               operation, metrics.total_long_delay_count);
//     }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let runtime_env = RuntimeEnvBuilder::default().build().unwrap();
    let ctx = Box::into_raw(Box::new(runtime_env)) as jlong;
    ctx
}
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
    runtime_id: jlong,
) -> jlong {
    let runtime_env = unsafe { Arc::from_raw(runtime_id as *const RuntimeEnv) };
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config_rt(config, runtime_env.clone());
    let _ = Arc::into_raw(runtime_env);

    let ctx = Box::into_raw(Box::new(context)) as jlong;
    ctx
}
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeSessionContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
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
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong
)  {
    let _ = unsafe { Box::from_raw(ptr as *mut ShardView) };
}
// #[no_mangle]
// pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_freeStream(
//     _env: JNIEnv,
//     _class: JClass,
//     stream_ptr: jlong,
// ) {
//     if stream_ptr != 0 {
//         unsafe {
//             let _ = Box::from_raw(stream_ptr as *mut SendableRecordBatchStream);
//         }
//     }
// }
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

// If we need to create session context separately
// TODO : not used, remove this
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_nativeCreateSessionContext(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    shard_view_ptr: jlong,
    global_runtime_env_ptr: jlong,
) -> jlong {
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let table_path = shard_view.table_path();
    let files_meta = shard_view.files_meta();
    // Will use it once the global RunTime is defined
    // let runtime_arc = unsafe {
    //     let boxed = &*(runtime_env_ptr as *const Pin<Arc<RuntimeEnv>>);
    //     (**boxed).clone()
    // };
    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(table_path.prefix(), files_meta);
    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default()
            .with_list_files_cache(Some(list_file_cache))).build().unwrap();
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), Arc::new(runtime_env));
    // Create default parquet options
    let file_format = CsvFormat::default();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".csv");
    let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    let mut session_context_ptr = 0;
    runtime.block_on(async {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path.clone())
            .await.unwrap();
        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        // Create a new TableProvider
        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        let shard_id = table_path.prefix().filename().expect("error in fetching Path");
        ctx.register_table(shard_id, provider)
            .expect("Failed to attach the Table");
        // Return back after wrapping in Box
        session_context_ptr = Box::into_raw(Box::new(ctx)) as jlong
    });
    session_context_ptr
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

    let stream = unsafe { &mut *(stream as *mut RecordBatchStreamAdapter<CrossRtStream>) };

//     let result = futures::executor::block_on(async {
//         STREAM_NEXT_MONITOR.instrument(stream.try_next()).await
//     });

    // Call directly - CrossRtStream handles CPU executor internally
    // Poll from IO runtime - actual work happens on CPU executor via CrossRtStream
    manager.io_runtime.block_on(async move{
        // Simple polling - CrossRtStream handles the runtime bridge
        let result = STREAM_NEXT_MONITOR.instrument(async {
            let res = stream.try_next().await;
            res
        }).await;

        match result {
            Ok(Some(batch)) => {
                // Convert to FFI on IO runtime (lightweight)
                let struct_array: StructArray = batch.into();
                let array_data = struct_array.into_data();
                let ffi_array = FFI_ArrowArray::new(&array_data);
                let ffi_array_ptr = Box::into_raw(Box::new(ffi_array));
                set_object_result_ok(&mut env, callback, ffi_array_ptr);
            }
            Ok(None) => {
                set_object_result_ok(&mut env, callback, 0 as *mut FFI_ArrowSchema);
            }
            Err(err) => {
                set_object_result_error(&mut env, callback, &err);
            }
        }
    });
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
    //let stream = unsafe { &mut *(stream_ptr as *mut SendableRecordBatchStream) };
    let schema = stream.schema();

    match FFI_ArrowSchema::try_from(&*schema) {
        Ok(mut ffi_schema) => {
            set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_schema));
        }
        Err(err) => {
            set_object_result_error(&mut env, callback, &err);
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
    mut env: JNIEnv,
    _class: JClass,
    stream: jlong
) {
    let _ = unsafe { Box::from_raw(stream as *mut SendableRecordBatchStream) };
}

// #[no_mangle]
// pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_closeStream(
//     _env: JNIEnv,
//     _class: JClass,
//     stream_ptr: jlong,
// ) {
//     if stream_ptr != 0 {
//         let _ = unsafe { Box::from_raw(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };
//     }
// }

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
    runtime: jlong
) {
    //let _ = unsafe { Arc::from_raw(runtime as *const RuntimeEnv) };
    let _ = unsafe { Box::from_raw(runtime as *mut RuntimeEnv) };
}

