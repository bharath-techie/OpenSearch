/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::pin::Pin;
use std::sync::Arc;
use datafusion::datasource::listing::ListingTable;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass};
use jni::sys::{jbyteArray, jlong};
use substrait::proto::Plan;
// How prost Message get's linked to plan::Decode?
use prost::Message;
use tokio::runtime::Runtime;

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_csv_Searcher_nativeExecuteSubstraitQuery(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    session_context_ptr: jlong,
    substrait_bytes: jbyteArray
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

    let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    runtime.block_on(async {

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
    });
    // Create DataFrame from the converted logical plan


}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_csv_Searcher_nativeCreateSessionContext(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    listing_table_ptr: jlong,
    runtime_env_ptr: jlong,
) {

    let listing_table = unsafe {
        let boxed = &*(runtime_env_ptr as *const Pin<Arc<ListingTable>>);
        (**boxed).clone()
    };

    let session_context = SessionContext::new();
}
