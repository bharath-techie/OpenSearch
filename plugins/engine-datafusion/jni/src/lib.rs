/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ptr::addr_of_mut;
use jni::objects::{JByteArray, JClass, JIntArray, JObject};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;
use std::sync::Arc;
use arrow_array::{Array, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use datafusion::common::DataFusionError;

mod util;
mod row_id_optimizer;
mod listing_table;

use datafusion::execution::context::SessionContext;

use crate::util::{create_object_meta_from_filenames, parse_string_arr, set_object_result, set_object_result_error, set_object_result_ok};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{ListingTableUrl};
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::cache_unit::DefaultListFilesCache;
use datafusion::execution::cache::CacheAccessor;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::SessionConfig;
use datafusion::DATAFUSION_VERSION;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccess};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::execution::TaskContext;
use datafusion::parquet::arrow::arrow_reader::RowSelector;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::source::DataSourceExec;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use futures::TryStreamExt;
use jni::objects::{JObjectArray, JString};
use object_store::ObjectMeta;
use parquet::file::reader::{FileReader, SerializedFileReader};
use prost::Message;
use tokio::runtime::Runtime;
use crate::listing_table::{ListingOptions, ListingTable, ListingTableConfig};
use crate::row_id_optimizer::FilterRowIdOptimizer;

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
    let rt = Runtime::new().unwrap();
    let ctx = Box::into_raw(Box::new(rt)) as jlong;
    ctx
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let runtime_env = RuntimeEnvBuilder::default().build().unwrap();
    /**
    // We can copy global runtime to local runtime - file statistics cache, and most of the things
    // will be shared across session contexts. But list files cache will be specific to session
    // context

    let fsCache = runtimeEnv.clone().cache_manager.get_file_statistic_cache().unwrap();
    let localCacheManagerConfig = CacheManagerConfig::default().with_files_statistics_cache(Option::from(fsCache));
    let localCacheManager = CacheManager::try_new(&localCacheManagerConfig);
    let localRuntimeEnv = RuntimeEnvBuilder::new()
        .with_cache_manager(localCacheManagerConfig)
        .with_disk_manager(DiskManagerConfig::new_existing(runtimeEnv.disk_manager))
        .with_memory_pool(runtimeEnv.memory_pool)
        .with_object_store_registry(runtimeEnv.object_store_registry)
        .build();
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config(config);
    **/

    let ctx = Box::into_raw(Box::new(runtime_env)) as jlong;
    ctx
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
    runtime_id: jlong,
) -> jlong {
    let runtimeEnv = unsafe { &mut *(runtime_id as *mut RuntimeEnv) };
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config_rt(config, Arc::new(runtimeEnv.clone()));
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
    substrait_bytes: jbyteArray,
    tokio_runtime_env_ptr: jlong,
    // callback: JObject,
) -> jlong {
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let runtime_ptr = unsafe { &*(tokio_runtime_env_ptr as *const Runtime)};

    let table_path = shard_view.table_path();
    let files_meta = shard_view.files_meta();


    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(table_path.prefix(), files_meta);

    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default()
            //.with_list_files_cache(Some(list_file_cache)) TODO: //Fix this
        ).build().unwrap();

    // TODO: get config from CSV DataFormat
    let mut config = SessionConfig::new();
    // config.options_mut().execution.parquet.pushdown_filters = true;

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        // .with_optimizer_rule(Arc::new(OptimizeRowId))
        // .with_physical_optimizer_rule(Arc::new(FilterRowIdOptimizer)) // TODO: enable only for query phase
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Create default parquet options
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet"); // TODO: take this as parameter
        // .with_table_partition_cols(vec![("row_base".to_string(), DataType::Int32)]); // TODO: enable only for query phase

    // Ideally the executor will give this
    runtime_ptr.block_on(async {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path.clone())
            .await.unwrap();


        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        // Create a new TableProvider
        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        let shard_id = table_path.prefix().filename().expect("error in fetching Path");
        ctx.register_table("logs", provider)
            .expect("Failed to attach the Table");

    });

    // TODO : how to close ctx ?
    // Convert Java byte array to Rust Vec<u8>
    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            let error_msg = format!("Failed to convert plan bytes: {}", e);
            env.throw_new("java/lang/Exception", error_msg);
            return 0;
        }
    };

    let substrait_plan = match Plan::decode(plan_bytes_vec.as_slice()) {
        Ok(plan) => {
            println!("SUBSTRAIT rust: Decoding is successful, Plan has {} relations", plan.relations.len());
            plan
        },
        Err(e) => {
            return 0;
        }
    };

    //let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    runtime_ptr.block_on(async {

        let logical_plan = match from_substrait_plan(&ctx.state(), &substrait_plan).await {
            Ok(plan) => {
                println!("SUBSTRAIT Rust: LogicalPlan: {:?}", plan);
                plan
            },
            Err(e) => {
                println!("SUBSTRAIT Rust: Failed to convert Substrait plan: {}", e);
                return 0;
            }
        };

        let dataframe = ctx.execute_logical_plan(logical_plan).await.unwrap();
        let stream = dataframe.execute_stream().await.unwrap();
        let stream_ptr = Box::into_raw(Box::new(stream)) as jlong;

        stream_ptr

    })
}

// If we need to create session context separately
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


    // let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    let mut session_context_ptr = 0;

    // Ideally the executor will give this
    Runtime::new().expect("Failed to create Tokio Runtime").block_on(async {
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
    let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };

    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    runtime.block_on(async {
        //let fetch_start = std::time::Instant::now();
        let next = stream.try_next().await;
        //let fetch_time = fetch_start.elapsed();
        match next {
            Ok(Some(batch)) => {
                //let convert_start = std::time::Instant::now();
                // Convert to struct array for compatibility with FFI
                //println!("Num rows : {}", batch.num_rows());
                let struct_array: StructArray = batch.into();
                let array_data = struct_array.into_data();
                let mut ffi_array = FFI_ArrowArray::new(&array_data);
                //let convert_time = convert_start.elapsed();
                // ffi_array must remain alive until after the callback is called
                // let callback_start = std::time::Instant::now();
                set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_array));
                // let callback_time = callback_start.elapsed();
                // println!("Fetch: {:?}, Convert: {:?}, Callback: {:?}",
                //          fetch_time, convert_time, callback_time);
            }
            Ok(None) => {
                set_object_result_ok(&mut env, callback, 0 as *mut FFI_ArrowSchema);
            }
            Err(err) => {
                set_object_result_error(&mut env, callback, &err);
            }
        }
        //println!("Total time: {:?}", start.elapsed());
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_getSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream: jlong,
    callback: JObject,
) {
    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    let schema = stream.schema();
    let ffi_schema = FFI_ArrowSchema::try_from(&*schema);
    match ffi_schema {
        Ok(mut ffi_schema) => {
            // ffi_schema must remain alive until after the callback is called
            set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_schema));
        }
        Err(err) => {
            set_object_result_error(&mut env, callback, &err);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_executeFetchPhase(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    values: JIntArray, // TODO : Add projections
    tokio_runtime_env_ptr: jlong,
    callback: JObject,
) {


    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let runtime_ptr = unsafe { &*(tokio_runtime_env_ptr as *const Runtime)};

    let table_path = shard_view.table_path();
    let files_meta = shard_view.files_meta();


    // Safety checks first
    if values.is_null() {
        let _ = env.throw_new("java/lang/NullPointerException", "values array is null");
        return;
    }

    // Get array length
    let array_length = match env.get_array_length(&values) {
        Ok(len) => len,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException",
                                  format!("Failed to get array length: {:?}", e));
            return;
        }
    };

    // Create a buffer to hold the array data
    let mut row_ids = vec![0; array_length as usize];

    // Copy the array data into the buffer
    match env.get_int_array_region(&values, 0, &mut row_ids) {
        Ok(_) => {
            // Now buffer contains the array data
            // println!("Received array of length {}: {:?}", array_length, row_ids);
        },
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException",
                                  format!("Failed to get array data: {:?}", e));
            return;
        }
    }


    // Safety checks
    if tokio_runtime_env_ptr == 0 {
        let error = DataFusionError::Execution("Null runtime pointer".to_string());
        set_object_result_error(&mut env, callback, &error);
        return;
    }

    let access_plans = create_access_plans(row_ids, files_meta.clone());


    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default()
                            //.with_list_files_cache(Some(list_file_cache)) TODO: //Fix this
        ).build().unwrap();
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), Arc::new(runtime_env));

    // Create default parquet options
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet"); // TODO: take this as parameter
    // .with_table_partition_cols(vec![("row_base".to_string(), DataType::Int32)]); // TODO: enable only for query phase

    // Ideally the executor will give this



    runtime_ptr.block_on(async {

        let parquet_schema = listing_options
            .infer_schema(&ctx.state(), &table_path.clone())
            .await.unwrap();

        let partitioned_files: Vec<PartitionedFile> = files_meta
            .iter()
            .zip(access_plans.await.iter())
            .map(|(meta, access_plan)| {
                PartitionedFile::new(
                    meta.location.clone(),
                    meta.size
                ).with_extensions(Arc::new(access_plan.clone()))
            })
            .collect();

        let file_group = FileGroup::new(partitioned_files);

        let file_source = Arc::new(
            ParquetSource::default()
            // provide the factory to create parquet reader without re-reading metadata
            //.with_parquet_file_reader_factory(Arc::new(reader_factory)),
        );

        let file_scan_config =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(),  parquet_schema.clone(), file_source)
                //.with_limit(limit)
                // .with_projection(projection)
                .with_file_group(file_group)
                .build();

        let parquet_exec = DataSourceExec::from_data_source(file_scan_config);

        // IMPORTANT: Only get one reference to each pointer
        // let liquid_ctx = unsafe { &mut *(context_ptr as *mut SessionContext) };
        // let session_ctx = unsafe { Box::from_raw(context_ptr as *mut SessionContext) };
        let mut optimized_plan: Arc<dyn ExecutionPlan> = parquet_exec.clone();



        let task_ctx = Arc::new(TaskContext::default());

        // let runtime = unsafe { &mut *(runtime as *mut Runtime) };
        // runtime.block_on(async {
        match optimized_plan.execute(0, task_ctx) {
            Ok(stream) => {
                let boxed_stream = Box::new(stream);
                let stream_ptr = Box::into_raw(boxed_stream);
                set_object_result_ok(
                    &mut env,
                    callback,
                    stream_ptr
                );
            }
            Err(e) => {
                set_object_result_error(
                    &mut env,
                    callback,
                    &e
                );
            }
        }
    });
}

async fn create_access_plans(
    row_ids: Vec<i32>,
    files_meta: Arc<Vec<ObjectMeta>>
) -> Result<Vec<ParquetAccessPlan>, DataFusionError> {
    let mut parquet_metadata = Vec::new();

    // First get ParquetMetaData for all files
    for meta in files_meta.iter() {
        let path = meta.location.as_ref();
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file).unwrap();
        parquet_metadata.push(reader.metadata().clone());
    }

    let mut access_plans = Vec::new();
    let mut cumulative_rows = 0;

    // Process each file
    for (file_idx, metadata) in parquet_metadata.iter().enumerate() {
        let total_row_groups = metadata.num_row_groups();
        let mut access_plan = ParquetAccessPlan::new_all(total_row_groups);

        // Calculate total rows in current file
        let mut file_total_rows = 0;
        let mut row_group_map: HashMap<usize, i32> = HashMap::new();
        for group_id in 0..total_row_groups {
            let rows_in_group = metadata.row_group(group_id).num_rows() as i32;
            row_group_map.insert(group_id, rows_in_group);
            file_total_rows += rows_in_group;
        }

        // Filter row IDs that belong to this file
        let file_row_ids: Vec<i32> = row_ids.iter()
            .filter(|&&id| id >= cumulative_rows && id < cumulative_rows + file_total_rows)
            .map(|&id| id - cumulative_rows) // Convert global row IDs to local
            .collect();

        if file_row_ids.is_empty() {
            // If no rows belong to this file, skip all row groups
            for group_id in 0..total_row_groups {
                access_plan.skip(group_id);
            }
        } else {
            // Group local row IDs by row group
            let mut group_map: HashMap<usize, BTreeSet<i32>> = HashMap::new();
            for row_id in file_row_ids {
                let rows_per_group = *row_group_map.get(&0).unwrap();
                group_map.entry((row_id / rows_per_group) as usize)
                    .or_default()
                    .insert(row_id % rows_per_group);
            }

            // Process each row group
            for group_id in 0..total_row_groups {
                let row_group_size = *row_group_map.get(&group_id).unwrap() as usize;
                if let Some(group_row_ids) = group_map.get(&group_id) {
                    let mut relative_row_ids: Vec<usize> = group_row_ids.iter()
                        .map(|&x| x as usize)
                        .collect();

                    if relative_row_ids.is_empty() {
                        access_plan.skip(group_id);
                    } else if relative_row_ids.len() == row_group_size {
                        access_plan.scan(group_id);
                    } else {
                        // Create selectors
                        let mut selectors = Vec::new();
                        let mut current_pos = 0;
                        let mut i = 0;
                        while i < relative_row_ids.len() {
                            let mut target_pos = relative_row_ids[i];
                            if target_pos > current_pos {
                                selectors.push(RowSelector::skip(target_pos - current_pos));
                            }
                            let mut select_count = 1;
                            while i + 1 < relative_row_ids.len() &&
                                relative_row_ids[i + 1] == relative_row_ids[i] + 1 {
                                select_count += 1;
                                i += 1;
                                target_pos = relative_row_ids[i];
                            }
                            selectors.push(RowSelector::select(select_count));
                            current_pos = relative_row_ids[i] + 1;
                            i += 1;
                        }
                        if current_pos < row_group_size {
                            selectors.push(RowSelector::skip(row_group_size - current_pos));
                        }
                        access_plan.set(group_id, RowGroupAccess::Selection(selectors.into()));
                    }
                } else {
                    access_plan.skip(group_id);
                }
            }
        }

        access_plans.push(access_plan);
        cumulative_rows += file_total_rows;
    }

    Ok(access_plans)
}
