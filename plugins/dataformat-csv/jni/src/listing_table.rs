/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use crate::util::{create_object_meta_from_filenames, parse_string_arr};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::execution::cache::CacheAccessor;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use jni::objects::{JClass, JObjectArray, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use std::pin::Pin;
use std::sync::Arc;
use datafusion::datasource::{provider, TableProvider};
use tokio::runtime::Runtime;

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_csv_ListingTable_createListingTable(
    mut env: JNIEnv,
    _class: JClass,
    directory_path: JString,
    files: JObjectArray,
    runtime_env_ptr: jlong,
    runtime_ptr: jlong,
    table_name: JString
) {

    if runtime_env_ptr == 0 {
        env.throw_new("java/lang/IllegalArgumentException", "Null pointer")
            .expect("Failed to throw exception");
        return;
    }

    // Need to see how to do it right for an Arc object
    let runtime_arc = unsafe {
        let boxed = &*(runtime_env_ptr as *const Pin<Arc<RuntimeEnv>>);
        (**boxed).clone()
    };


    // How to throw the expect code error?
    let table_path: String = env
        .get_string(&directory_path)
        .expect("Couldn't get java String")
        .into();

    let table_name: String = env
        .get_string(&table_name)
        .expect("Couldn't get java String")
        .into();

    let files = parse_string_arr(&mut env, files)
        .expect("Expected list of file meta")
        .into();
    let files_meta = create_object_meta_from_filenames(&table_path, files);
    let table_path = ListingTableUrl::parse(table_path).unwrap();

    // Initiate the listing table with the files
    let list_file_cache = runtime_arc.cache_manager.get_list_files_cache()
        .expect("Cache should be present");
    list_file_cache.put(table_path.prefix(), Arc::new(files_meta));

    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), Arc::new(runtime_arc));


    // Create default parquet options
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet");


    let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    runtime.block_on(async {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path)
            .await.unwrap();


        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        // Create a new TableProvider
        let provider = Arc::new(ListingTable::try_new(config).unwrap());

        // Return back after wrapping in Box
        Box::into_raw(Box::new(provider)) as jlong
    });

}

pub extern "system" fn Java_org_opensearch_datafusion_csv_ListingTable_destroyListingTable(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    // We can also do ListingTable instead of dyn TableProvider... need to see which one is better?
    let _ = unsafe { Box::from_raw(pointer as *mut Arc<dyn TableProvider>) };
}
