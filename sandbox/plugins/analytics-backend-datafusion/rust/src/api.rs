/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Bridge-agnostic API layer.
//!
//! All functions in this module use plain Rust types — no FFI-specific types.
//! The FFM bridge (`ffm.rs`) calls into these functions directly.
//!
//! # Pointer contract
//!
//! Functions that accept `i64` pointer arguments require non-zero, valid pointers
//! to the corresponding Rust type. The caller (bridge layer) is responsible for
//! null-checking before calling. Functions that return `i64` return heap-allocated
//! pointers via `Box::into_raw`; the caller owns the pointer and must call the
//! corresponding close function exactly once.
//!
//! # Thread safety
//!
//! - `init_runtime_manager` and `shutdown_runtime_manager` must be called from a
//!   single thread (node startup/shutdown).
//! - `create_global_runtime` / `close_global_runtime` are not thread-safe for the
//!   same pointer.
//! - `execute_query`: async. Safe to call concurrently with different shard/runtime pointers.
//!   The bridge layer wraps with `block_on` or `spawn`.
//! - `stream_next`: async. The bridge layer wraps with `block_on` or `spawn`.
//! - `stream_get_schema`, `stream_close` must NOT be called
//!   concurrently on the same stream pointer.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Array, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{GreedyMemoryPool, TrackConsumersPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::execution::RecordBatchStream;
use datafusion::prelude::SessionConfig;
use futures::TryStreamExt;

use crate::cross_rt_stream::CrossRtStream;
use crate::query_memory_pool_tracker::QueryTrackingContext;
use crate::runtime_manager::RuntimeManager;

/// Bundles a stream with its query tracking context so that dropping the
/// handle automatically marks the query completed in the registry.
pub struct QueryStreamHandle {
    stream: RecordBatchStreamAdapter<CrossRtStream>,
    /// Held for its `Drop` impl — marks the query completed when the
    /// stream is closed.
    _query_tracking_context: QueryTrackingContext,
}

impl QueryStreamHandle {
    pub fn new(stream: RecordBatchStreamAdapter<CrossRtStream>, query_context: QueryTrackingContext) -> Self {
        Self { stream, _query_tracking_context: query_context }
    }
}

/// Build ObjectMeta for each file using the given object store.
pub async fn create_object_metas(
    store: &dyn object_store::ObjectStore,
    base_path: &str,
    filenames: Vec<String>,
) -> Result<Vec<object_store::ObjectMeta>, DataFusionError> {
    let mut metas = Vec::with_capacity(filenames.len());
    for filename in filenames {
        let full_path = if filename.starts_with('/') || filename.contains(base_path) {
            filename
        } else {
            format!("{}/{}", base_path.trim_end_matches('/'), filename)
        };
        let path = object_store::path::Path::from(full_path.as_str());
        let meta = store.head(&path).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get object meta for {}: {}", full_path, e))
        })?;
        metas.push(meta);
    }
    Ok(metas)
}

/// Opaque runtime handle returned to the caller.
/// Contains the DataFusion RuntimeEnv (memory pool, disk spill, cache).
pub struct DataFusionRuntime {
    pub runtime_env: datafusion::execution::runtime_env::RuntimeEnv,
}

/// Opaque shard view handle returned to the caller.
pub struct ShardView {
    pub table_path: ListingTableUrl,
    pub object_metas: Arc<Vec<object_store::ObjectMeta>>,
}

/// Creates a DataFusion global runtime with the given resource limits.
///
/// Returns a heap-allocated pointer (as i64) to `DataFusionRuntime`.
/// Caller must call `close_global_runtime` exactly once to free it.
pub fn create_global_runtime(
    memory_pool_limit: i64,
    spill_dir: &str,
    spill_limit: i64,
) -> Result<i64, DataFusionError> {
    let disk_manager = DiskManagerBuilder::default()
        .with_max_temp_directory_size(spill_limit as u64)
        .with_mode(DiskManagerMode::Directories(vec![PathBuf::from(spill_dir)]));

    let memory_pool = Arc::new(TrackConsumersPool::new(
        GreedyMemoryPool::new(memory_pool_limit as usize),
        NonZeroUsize::new(5).unwrap(),
    ));

    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(memory_pool)
        .with_disk_manager_builder(disk_manager)
        .build()?;

    let runtime = DataFusionRuntime { runtime_env };
    Ok(Box::into_raw(Box::new(runtime)) as i64)
}

/// Closes a DataFusion global runtime. Safe to call with 0 (no-op).
///
/// # Safety
/// `ptr` must be 0 or a valid pointer returned by `create_global_runtime`.
pub unsafe fn close_global_runtime(ptr: i64) {
    if ptr != 0 {
        let _ = Box::from_raw(ptr as *mut DataFusionRuntime);
    }
}

/// Creates a native reader (ShardView) for the given path and files.
///
/// Returns a heap-allocated pointer (as i64) to `ShardView`.
/// Caller must call `close_reader` exactly once to free it.
pub fn create_reader(
    table_path: &str,
    mut filenames: Vec<String>,
    tokio_rt_manager: &RuntimeManager,
) -> Result<i64, DataFusionError> {
    filenames.sort();

    let table_url = ListingTableUrl::parse(table_path)
        .map_err(|e| DataFusionError::Execution(format!("Invalid table path: {}", e)))?;

    // TODO: use global runtime's object store instead of building a throwaway RuntimeEnv
    let default_rt = RuntimeEnvBuilder::new().build()?;
    let store = default_rt.object_store(&table_url)?;

    let object_metas = tokio_rt_manager.io_runtime.block_on(
        create_object_metas(store.as_ref(), table_path, filenames),
    )?;

    let shard_view = ShardView {
        table_path: table_url,
        object_metas: Arc::new(object_metas),
    };
    Ok(Box::into_raw(Box::new(shard_view)) as i64)
}

/// Closes a native reader. Safe to call with 0 (no-op).
///
/// # Safety
/// `ptr` must be 0 or a valid pointer returned by `create_reader`.
pub unsafe fn close_reader(ptr: i64) {
    if ptr != 0 {
        let _ = Box::from_raw(ptr as *mut ShardView);
    }
}

/// Executes a query. Returns a heap-allocated pointer (as i64) to the result stream.
/// Caller must call `stream_close` exactly once to free it.
///
/// This is an async function — the bridge layer decides how to run it
/// (`block_on` for synchronous delivery, `spawn` for async delivery).
///
/// # Safety
/// `shard_view_ptr` and `runtime_ptr` must be valid, non-zero pointers.
pub async unsafe fn execute_query(
    shard_view_ptr: i64,
    table_name: &str,
    plan_bytes: &[u8],
    runtime_ptr: i64,
    manager: &RuntimeManager,
    context_id: i64,
    query_config: crate::datafusion_query_config::DatafusionQueryConfig,
) -> Result<i64, DataFusionError> {
    let shard_view = &*(shard_view_ptr as *const ShardView);
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let cpu_executor = manager.cpu_executor();

    // Create per-query context — auto-registers in the global registry
    let global_pool = runtime.runtime_env.memory_pool.clone();
    let query_context = QueryTrackingContext::new(context_id, global_pool);
    let query_memory_pool = query_context.memory_pool()
        .map(|p| p as Arc<dyn datafusion::execution::memory_pool::MemoryPool>);

    // Peek at the substrait extensions list to see if this is an indexed query.
    // The `index_filter` UDF name appears there if Calcite planted any
    // index_filter(bytes) calls. Cheap — just bytes inspection.
    let is_indexed = plan_bytes_mentions_index_filter(plan_bytes);

    let stream_ptr = if is_indexed {
        let qc = Arc::new(query_config);
        crate::indexed_executor::execute_indexed_query(
            plan_bytes.to_vec(),
            table_name.to_string(),
            shard_view,
            qc.target_partitions.max(1),
            runtime,
            cpu_executor,
            query_memory_pool,
            qc,
        )
        .await?
    } else {
        crate::query_executor::execute_query(
            shard_view.table_path.clone(),
            shard_view.object_metas.clone(),
            table_name.to_string(),
            plan_bytes.to_vec(),
            runtime,
            cpu_executor,
            query_memory_pool,
            &query_config,
        )
        .await?
    };

    // Reconstruct the stream from the raw pointer returned by the executor.
    let stream = *Box::from_raw(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>);
    let handle = QueryStreamHandle::new(stream, query_context);
    Ok(Box::into_raw(Box::new(handle)) as i64)
}

/// Cheap check: scan the substrait plan bytes for the `index_filter` function
/// name. If the planner emitted any `index_filter(bytes)` UDF call, the name
/// will be present in the plan's extension declarations.
///
/// False positives take the indexed path and then fail in
/// `execute_indexed_query` when `classify_filter` returns `None`
/// ("execute_indexed_query called with no index_filter(...) in plan"). There
/// is no automatic retry on the vanilla path — a false positive is a hard
/// query error. In practice this is unreachable because the needle is not a
/// valid DataFusion identifier anywhere else a plan would naturally contain
/// it; the failure mode is documented here to keep the dispatch contract
/// explicit.
fn plan_bytes_mentions_index_filter(plan_bytes: &[u8]) -> bool {
    // The substrait plan carries extension-function names as UTF-8 strings.
    // Substring match is sufficient for dispatch.
    const NEEDLE: &[u8] = b"index_filter";
    plan_bytes.windows(NEEDLE.len()).any(|w| w == NEEDLE)
}

/// Returns the Arrow schema for the given stream as a heap-allocated FFI_ArrowSchema pointer.
///
/// # Safety
/// `stream_ptr` must be a valid, non-zero pointer to a QueryStreamHandle.
pub unsafe fn stream_get_schema(stream_ptr: i64) -> Result<i64, DataFusionError> {
    let handle = &mut *(stream_ptr as *mut QueryStreamHandle);
    let schema = handle.stream.schema();
    let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())
        .map_err(|e| DataFusionError::Execution(format!("Schema conversion failed: {}", e)))?;
    Ok(Box::into_raw(Box::new(ffi_schema)) as i64)
}

/// Loads the next record batch from the stream.
///
/// Returns a heap-allocated FFI_ArrowArray pointer (as i64), or 0 if end-of-stream.
///
/// This is an async function — the bridge layer decides how to run it.
///
/// # Safety
/// `stream_ptr` must be a valid, non-zero pointer. Must not be called concurrently
/// on the same stream.
pub async unsafe fn stream_next(
    stream_ptr: i64,
) -> Result<i64, DataFusionError> {
    let handle = &mut *(stream_ptr as *mut QueryStreamHandle);

    let result = handle.stream.try_next().await?;

    match result {
        Some(batch) => {
            let struct_array: StructArray = batch.into();
            let array_data = struct_array.into_data();
            let ffi_array = FFI_ArrowArray::new(&array_data);
            Ok(Box::into_raw(Box::new(ffi_array)) as i64)
        }
        None => Ok(0),
    }
}

/// Closes a result stream. Safe to call with 0 (no-op).
///
/// # Safety
/// `stream_ptr` must be 0 or a valid pointer returned by `execute_query`.
pub unsafe fn stream_close(stream_ptr: i64) {
    if stream_ptr != 0 {
        // Dropping the handle drops both the stream and the query context.
        // The context's Drop impl marks the query completed in the registry.
        let _ = Box::from_raw(stream_ptr as *mut QueryStreamHandle);
    }
}

/// Converts SQL to Substrait plan bytes (test only).
///
/// # Safety
/// `shard_view_ptr` and `runtime_ptr` must be valid, non-zero pointers.
pub unsafe fn sql_to_substrait(
    shard_view_ptr: i64,
    table_name: &str,
    sql: &str,
    runtime_ptr: i64,
    manager: &RuntimeManager,
) -> Result<Vec<u8>, DataFusionError> {
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::execution::cache::{CacheAccessor, DefaultListFilesCache};
    use datafusion::execution::cache::cache_manager::CacheManagerConfig;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;

    let shard_view = &*(shard_view_ptr as *const ShardView);
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let table_path = shard_view.table_path.clone();
    let object_metas = shard_view.object_metas.clone();
    let table_name = table_name.to_string();

    manager.io_runtime.block_on(async {
        let list_file_cache = Arc::new(DefaultListFilesCache::default());
        list_file_cache.put(
            &datafusion::execution::cache::TableScopedPath {
                table: None,
                path: table_path.prefix().clone(),
            },
            object_metas,
        );
        let runtime_env = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
            .with_cache_manager(
                CacheManagerConfig::default()
                    .with_list_files_cache(Some(list_file_cache))
                    .with_file_metadata_cache(Some(
                        runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                    ))
                    .with_files_statistics_cache(
                        runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                    ),
            )
            .build()?;

        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_runtime_env(Arc::from(runtime_env))
            .with_default_features()
            .build();
        let ctx = datafusion::prelude::SessionContext::new_with_state(state);

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet")
            .with_collect_stat(true);
        let schema = listing_options.infer_schema(&ctx.state(), &table_path).await?;
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);
        ctx.register_table(&table_name, Arc::new(ListingTable::try_new(config)?))?;

        let plan = ctx.sql(sql).await?.logical_plan().clone();
        let substrait = to_substrait_plan(&plan, &ctx.state())?;
        let mut buf = Vec::new();
        substrait.encode(&mut buf)
            .map_err(|e| DataFusionError::Execution(format!("Substrait encode failed: {}", e)))?;
        Ok(buf)
    })
}
