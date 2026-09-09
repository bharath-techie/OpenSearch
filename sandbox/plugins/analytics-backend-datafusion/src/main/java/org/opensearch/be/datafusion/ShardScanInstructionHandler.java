/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FastPathHintSpec;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.be.datafusion.nativelib.FastPathHints;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Handles ShardScan instruction: creates a SessionContext via FFM and registers
 * the default ListingTable provider for parquet scans.
 */
public class ShardScanInstructionHandler implements FragmentInstructionHandler<ShardScanInstructionNode> {

    private final DataFusionPlugin plugin;

    ShardScanInstructionHandler(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public BackendExecutionContext apply(
        ShardScanInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        ShardScanExecutionContext context = (ShardScanExecutionContext) commonContext;
        DataFusionService dataFusionService = plugin.getDataFusionService();
        DataFormatRegistry registry = plugin.getDataFormatRegistry();

        DatafusionReader dfReader = null;
        for (String formatName : plugin.getSupportedFormats()) {
            dfReader = context.getReader().getReader(registry.format(formatName), DatafusionReader.class);
            if (dfReader != null) break;
        }
        if (dfReader == null) {
            throw new IllegalStateException("No DatafusionReader available in the acquired reader");
        }

        long readerPtr = dfReader.getReaderHandle().getPointer();
        long runtimePtr = dataFusionService.getNativeRuntime().get();
        long contextId = context.getTask() != null ? context.getTask().getId() : 0L;
        // The coordinator captured the logical table name (alias / index pattern / index the query
        // referenced) from the plan's table-scan leaf. Register the shard's table under it so the
        // Substrait plan's NamedTable binds. Fall back to the concrete shard index name when absent.
        String tableName = node.getLogicalTableName() != null ? node.getLogicalTableName() : context.getTableName();

        WireConfigSnapshot snapshot = plugin.getDatafusionSettings().getSnapshot();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);
            // Planner fast-path hints, stamped onto the context from the FAST_PATH_HINTS instruction.
            MemorySegment hintsSegment = arena.allocate(FastPathHints.BYTE_SIZE);
            FastPathHints.fromSpec(context.getFastPathHints()).writeTo(hintsSegment);

            // Per-shard hasDeletions signal (#22910): when set, the pure-DF scan routes through the
            // indexed SingleCollector path so the injected match-all Collector excludes deleted rows.
            boolean deletedDocFilteringRequired = context.hasDeletedDocs();

            SessionContextHandle sessionCtxHandle;
            if (route(context, node.requestsRowIds()) == ScanRoute.INDEXED) {
                // One indexed path serves three orthogonal reasons to leave the vanilla ListingTable:
                // QTF row-ids, delete filtering (#22910 — deletions force CONJUNCTIVE), and a planner
                // fast-path shape. No delegated predicates flow through this handler, so count = 0.
                int treeShape = deletedDocFilteringRequired
                    ? FilterTreeShape.CONJUNCTIVE.ordinal()
                    : FilterTreeShape.NO_DELEGATION.ordinal();
                sessionCtxHandle = NativeBridge.createSessionContextForIndexedExecution(
                    readerPtr,
                    runtimePtr,
                    tableName,
                    contextId,
                    treeShape,
                    0,
                    node.requestsRowIds(),
                    deletedDocFilteringRequired,
                    context.hasPartialAggregate(),
                    segment.address(),
                    context.getFragmentBytes(),
                    hintsSegment.address()
                );
            } else {
                // Plan bytes let Rust widen the schema for multi-index queries (null-fill missing columns).
                sessionCtxHandle = NativeBridge.createSessionContext(
                    readerPtr,
                    runtimePtr,
                    tableName,
                    contextId,
                    false,
                    context.hasPartialAggregate(),
                    segment.address(),
                    context.getFragmentBytes()
                );
            }
            return new DataFusionSessionState(sessionCtxHandle);
        }
    }

    /** Where a base shard scan runs: the vanilla ListingTable, or the indexed SingleCollector path. */
    enum ScanRoute {
        LISTING,
        INDEXED
    }

    /**
     * Folds the three reasons a base shard scan needs the indexed path into one decision: QTF row-id
     * emission, per-shard delete filtering (#22910), or a planner fast-path shape (read from the
     * context, where {@code AnalyticsSearchService} stamped it). Any one routes {@link ScanRoute#INDEXED};
     * otherwise the vanilla {@link ScanRoute#LISTING} path runs with zero extra work.
     */
    static ScanRoute route(ShardScanExecutionContext context, boolean requestsRowIds) {
        boolean fastPath = context.getFastPathHints().shape() != FastPathHintSpec.Shape.NONE;
        if (requestsRowIds || context.hasDeletedDocs() || fastPath) {
            return ScanRoute.INDEXED;
        }
        return ScanRoute.LISTING;
    }
}
