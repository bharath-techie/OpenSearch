/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::sync::Arc;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::cache::cache_manager::ListFilesCache;
use datafusion::execution::cache::cache_unit::DefaultListFilesCache;
use datafusion::execution::cache::CacheAccessor;
use jni::JNIEnv;
use jni::objects::{JClass, JObjectArray, JString};
use jni::sys::jlong;
use object_store::ObjectMeta;
use crate::util::{create_object_meta_from_filenames, parse_string_arr};

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_csv_ShardView_createShardView(
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
pub extern "system" fn Java_org_opensearch_datafusion_csv_ShardView_destroyShardView(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong
)  {
    let _ = unsafe { Box::from_raw(ptr as *mut ShardView) };
}

pub struct ShardView {
    table_path: ListingTableUrl,
    files_meta: Vec<ObjectMeta>
}

impl ShardView {
    pub fn new(table_path: ListingTableUrl, files_meta: Vec<ObjectMeta>) -> Self {
        ShardView {
            table_path,
            files_meta
        }
    }

    pub fn table_path(self) -> ListingTableUrl {
        self.table_path
    }

    pub fn files_meta(self) -> Vec<ObjectMeta> {
        self.files_meta
    }
}
