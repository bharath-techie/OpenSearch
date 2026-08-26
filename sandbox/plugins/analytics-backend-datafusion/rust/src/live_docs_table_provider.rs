/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Deleted-document filtering for the vanilla DataFusion ListingTable path.
//!
//! Parquet `RowSelection` performs poorly when deletes are scattered because it turns a
//! contiguous scan into thousands of tiny select/skip runs. This provider keeps Parquet's
//! vectorized full-file read path and applies the Lucene liveDocs bitmap to decoded Arrow
//! batches instead. Files remain in one ordered scan partition; cumulative segment boundaries
//! map each batch's global row offset back to segment-local Lucene doc IDs.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::BooleanBuilder;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableType;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::table_schema::TableSchema;
use datafusion_datasource::PartitionedFile;
use futures::StreamExt;
use object_store::ObjectMeta;

use crate::indexed_table::{ffm_callbacks::get_live_docs, parquet_bridge};

/// Per-file information needed to fetch the segment's liveDocs bitmap.
pub struct LiveDocsFileInfo {
    pub object_meta: ObjectMeta,
    pub writer_generation: i64,
}

pub struct LiveDocsTableProvider {
    schema: SchemaRef,
    files: Vec<LiveDocsFileInfo>,
    store_url: ObjectStoreUrl,
    store: Arc<dyn object_store::ObjectStore>,
    metadata_cache: Arc<dyn FileMetadataCache>,
    context_id: i64,
}

impl LiveDocsTableProvider {
    pub fn new(
        schema: SchemaRef,
        files: Vec<LiveDocsFileInfo>,
        store_url: ObjectStoreUrl,
        store: Arc<dyn object_store::ObjectStore>,
        metadata_cache: Arc<dyn FileMetadataCache>,
        context_id: i64,
    ) -> Self {
        Self {
            schema,
            files,
            store_url,
            store,
            metadata_cache,
            context_id,
        }
    }
}

impl fmt::Debug for LiveDocsTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveDocsTableProvider")
            .field("files", &self.files.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for LiveDocsTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // The scan itself does not evaluate predicates. Keeping them above this provider is
        // important: liveDocs must be applied while batch offsets still match physical rows.
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut partitioned_files = Vec::with_capacity(self.files.len());
        let mut segments = Vec::with_capacity(self.files.len());
        let mut segment_start = 0usize;

        for file_info in &self.files {
            // Shard-view metadata is optional and is absent after restart. Load the authoritative
            // row count from the cached Parquet footer so max_doc never silently becomes zero.
            let (_, _, parquet_metadata) = parquet_bridge::load_parquet_metadata_with_meta(
                Arc::clone(&self.store),
                &file_info.object_meta.location,
                file_info.object_meta.clone(),
                Arc::clone(&self.metadata_cache),
            )
            .await
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "failed to load Parquet metadata for writer_generation={}: {}",
                    file_info.writer_generation, error
                ))
            })?;
            let num_rows_u64 =
                parquet_metadata
                    .row_groups()
                    .iter()
                    .try_fold(0u64, |total, row_group| {
                        let rows = u64::try_from(row_group.num_rows()).map_err(|_| {
                            DataFusionError::Execution(format!(
                                "negative Parquet row count for writer_generation={}",
                                file_info.writer_generation
                            ))
                        })?;
                        total.checked_add(rows).ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "Parquet row count overflow for writer_generation={}",
                                file_info.writer_generation
                            ))
                        })
                    })?;
            let num_rows = usize::try_from(num_rows_u64).map_err(|_| {
                DataFusionError::Execution(format!(
                    "segment {} row count {} does not fit usize",
                    file_info.writer_generation, num_rows_u64
                ))
            })?;
            let max_doc = i32::try_from(num_rows).map_err(|_| {
                DataFusionError::Execution(format!(
                    "segment {} row count {} exceeds Lucene doc-id range",
                    file_info.writer_generation, num_rows
                ))
            })?;

            let live_docs = get_live_docs(self.context_id, file_info.writer_generation, 0, max_doc)
                .map_err(|error| {
                    // Never fail open: returning unfiltered parquet rows would resurrect deletes.
                    DataFusionError::Execution(format!(
                        "failed to fetch liveDocs for writer_generation={}: {}",
                        file_info.writer_generation, error
                    ))
                })?
                .map(Arc::new);
            let segment_end = segment_start.checked_add(num_rows).ok_or_else(|| {
                DataFusionError::Execution("combined segment row count overflow".to_string())
            })?;
            segments.push(SegmentLiveDocs {
                start: segment_start,
                end: segment_end,
                words: live_docs,
            });
            segment_start = segment_end;
            partitioned_files.push(PartitionedFile::from(file_info.object_meta.clone()));
        }

        // Keep the same single, ordered file group used by the original ListingTable path.
        // DataFusion may coalesce file groups, so correctness cannot rely on partition index
        // identifying a segment. Segment boundaries below map the combined row offset instead.
        let file_groups = vec![FileGroup::new(partitioned_files)];
        let table_schema = TableSchema::new(Arc::clone(&self.schema), vec![]);
        let parquet_source = ParquetSource::new(table_schema);
        let mut builder =
            FileScanConfigBuilder::new(self.store_url.clone(), Arc::new(parquet_source))
                .with_file_groups(file_groups);

        if let Some(projection) = projection {
            builder = builder
                .with_projection_indices(Some(projection.clone()))
                .map_err(|error| DataFusionError::Internal(error.to_string()))?;
        }

        let scan = DataSourceExec::from_data_source(builder.build());
        Ok(Arc::new(LiveDocsFilterExec::try_new(
            scan,
            Arc::new(segments),
            segment_start,
        )?))
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

#[derive(Clone)]
struct SegmentLiveDocs {
    start: usize,
    end: usize,
    words: Option<Arc<Vec<u64>>>,
}

/// Applies segment-local liveDocs bitmaps to the ordered, combined Parquet scan.
struct LiveDocsFilterExec {
    input: Arc<dyn ExecutionPlan>,
    segments: Arc<Vec<SegmentLiveDocs>>,
    expected_rows: usize,
    properties: Arc<PlanProperties>,
}

impl LiveDocsFilterExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        segments: Arc<Vec<SegmentLiveDocs>>,
        expected_rows: usize,
    ) -> Result<Self> {
        let partitions = input.properties().output_partitioning().partition_count();
        if partitions != 1 {
            return Err(DataFusionError::Internal(format!(
                "liveDocs requires one ordered scan partition, got {}",
                partitions
            )));
        }
        let properties = Arc::clone(input.properties());
        Ok(Self {
            input,
            segments,
            expected_rows,
            properties,
        })
    }
}

impl fmt::Debug for LiveDocsFilterExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveDocsFilterExec")
            .field("segments", &self.segments.len())
            .field("expected_rows", &self.expected_rows)
            .finish()
    }
}

impl DisplayAs for LiveDocsFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LiveDocsFilterExec: segments={}, rows={}",
            self.segments.len(),
            self.expected_rows
        )
    }
}

impl ExecutionPlan for LiveDocsFilterExec {
    fn name(&self) -> &str {
        "LiveDocsFilterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "LiveDocsFilterExec expects one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::try_new(
            children.remove(0),
            Arc::clone(&self.segments),
            self.expected_rows,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "liveDocs ordered scan has no partition {}",
                partition
            )));
        }
        let input = self.input.execute(partition, context)?;
        if self.segments.iter().all(|segment| segment.words.is_none()) {
            return Ok(input);
        }

        let schema = input.schema();
        let segments = Arc::clone(&self.segments);
        let expected_rows = self.expected_rows;
        let mut row_offset = 0usize;
        let mapped = input.map(move |result| {
            let batch = result?;
            let filtered = filter_batch(&batch, &segments, row_offset, expected_rows)?;
            row_offset = row_offset.checked_add(batch.num_rows()).ok_or_else(|| {
                DataFusionError::Execution("liveDocs row offset overflow".to_string())
            })?;
            Ok(filtered)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
    }
}

fn filter_batch(
    batch: &RecordBatch,
    segments: &[SegmentLiveDocs],
    row_offset: usize,
    expected_rows: usize,
) -> Result<RecordBatch> {
    let end = row_offset
        .checked_add(batch.num_rows())
        .ok_or_else(|| DataFusionError::Execution("liveDocs batch range overflow".to_string()))?;
    if end > expected_rows {
        return Err(DataFusionError::Execution(format!(
            "parquet emitted rows [{}, {}) beyond combined segment row count {}",
            row_offset, end, expected_rows
        )));
    }

    let mut segment_index = segments.partition_point(|segment| segment.end <= row_offset);
    let mut mask = BooleanBuilder::with_capacity(batch.num_rows());
    let mut all_live = true;
    for row in row_offset..end {
        while segment_index < segments.len() && row >= segments[segment_index].end {
            segment_index += 1;
        }
        let segment = segments.get(segment_index).ok_or_else(|| {
            DataFusionError::Execution(format!("no liveDocs segment contains combined row {}", row))
        })?;
        if row < segment.start {
            return Err(DataFusionError::Execution(format!(
                "gap in liveDocs segment map before combined row {}",
                row
            )));
        }
        let local_row = row - segment.start;
        let live = match &segment.words {
            None => true,
            Some(words) => {
                let word = words.get(local_row >> 6).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "liveDocs bitmap for segment [{}, {}) is too short for local row {}",
                        segment.start, segment.end, local_row
                    ))
                })?;
                (word & (1u64 << (local_row & 63))) != 0
            }
        };
        all_live &= live;
        mask.append_value(live);
    }
    if all_live {
        return Ok(batch.clone());
    }
    filter_record_batch(batch, &mask.finish())
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn batch(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap()
    }

    #[test]
    fn filters_scattered_deletes_across_batches_and_segments() {
        // Segment 1 live rows: 0, 2, 3. Segment 2 live rows: 0, 2, 3, 4.
        let segments = vec![
            SegmentLiveDocs {
                start: 0,
                end: 5,
                words: Some(Arc::new(vec![0b0_1101u64])),
            },
            SegmentLiveDocs {
                start: 5,
                end: 10,
                words: Some(Arc::new(vec![0b1_1101u64])),
            },
        ];
        // Deliberately cross the segment boundary in the first batch.
        let first = filter_batch(&batch((0..7).collect()), &segments, 0, 10).unwrap();
        let second = filter_batch(&batch((7..10).collect()), &segments, 7, 10).unwrap();

        let first = first
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let second = second
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            (0..first.len()).map(|i| first.value(i)).collect::<Vec<_>>(),
            vec![0, 2, 3, 5]
        );
        assert_eq!(
            (0..second.len())
                .map(|i| second.value(i))
                .collect::<Vec<_>>(),
            vec![7, 8, 9]
        );
    }

    #[test]
    fn all_live_is_unchanged_and_short_bitmap_fails_closed() {
        let input = batch(vec![10, 11, 12]);
        let all_live = vec![SegmentLiveDocs {
            start: 0,
            end: 3,
            words: None,
        }];
        let output = filter_batch(&input, &all_live, 0, 3).unwrap();
        assert_eq!(output, input);

        let short = vec![SegmentLiveDocs {
            start: 0,
            end: 65,
            words: Some(Arc::new(vec![u64::MAX])),
        }];
        let error = filter_batch(&batch(vec![1]), &short, 64, 65).unwrap_err();
        assert!(error.to_string().contains("too short for local row 64"));
    }
}
