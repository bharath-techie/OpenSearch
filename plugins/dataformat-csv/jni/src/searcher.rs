/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use crate::shard_view::ShardView;
use crate::util::{set_object_result_error, set_object_result_ok};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::cache_unit::DefaultListFilesCache;
use datafusion::execution::cache::CacheAccessor;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use jni::objects::{JByteArray, JClass, JObject};
use jni::sys::{jbyteArray, jlong};
use jni::JNIEnv;
// How prost Message get's linked to plan::Decode?
use prost::Message;
use std::sync::Arc;
use substrait::proto::Plan;
use tokio::runtime::Runtime;

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_csv_Searcher_nativeExecuteSubstraitQuery(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    session_context_ptr: jlong,
    substrait_bytes: jbyteArray,
    // callback: JObject,
) {
    let ctx = unsafe { &mut *(session_context_ptr as *mut SessionContext) };
    // Convert Java byte array to Rust Vec<u8>
    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            let error_msg = format!("Failed to convert plan bytes: {}", e);
            env.throw_new("java/lang/Exception", error_msg);
            return;
        }
    };

    let substrait_plan = match Plan::decode(plan_bytes_vec.as_slice()) {
        Ok(plan) => {
            println!("SUBSTRAIT rust: Decoding is successful, Plan has {} relations", plan.relations.len());
            plan
        },
        Err(e) => {
            return;
        }
    };

    //let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    Runtime::new().expect("Failed to create Tokio Runtime").block_on(async {

        let logical_plan = match from_substrait_plan(&ctx.state(), &substrait_plan).await {
            Ok(plan) => {
                println!("SUBSTRAIT Rust: LogicalPlan: {:?}", plan);
                plan
            },
            Err(e) => {
                println!("SUBSTRAIT Rust: Failed to convert Substrait plan: {}", e);
                return;
            }
        };

        let dataframe = ctx.execute_logical_plan(logical_plan)
            .await.expect("Failed to run Logical Plan");

        dataframe.show().await.expect("Failed to get plan")
        // let task_ctx = Arc::new(dataframe.task_ctx());
        // let execution_plan = dataframe.create_physical_plan().await.expect("Failed to create Execution Plan");

        // match execution_plan.execute(0, task_ctx) {
        //     Ok(stream) => {
        //         let boxed_stream = Box::new(stream);
        //         let stream_ptr = Box::into_raw(boxed_stream);
        //         set_object_result_ok(
        //             &mut env,
        //             callback,
        //             stream_ptr
        //         );
        //     },
        //     Err(e) => {
        //         set_object_result_error(
        //             &mut env,
        //             callback,
        //             &e
        //         );
        //     }
        // }
    });
    // Create DataFrame from the converted logical plan


}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_csv_Searcher_nativeCreateSessionContext(
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
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet");


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
