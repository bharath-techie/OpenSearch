/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.sink;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.ArrowValues;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.StateSpecConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.transport.client.Client;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Terminal sink that streams the analytics engine's result into a target index: each
 * {@link #feed fed} Arrow batch is converted to bulk {@link IndexRequest}s and the batch is
 * released immediately — nothing accumulates on the coordinator, so result size is bounded
 * by the bulk buffer, not by a row cap. This is the "streaming result sink" shared
 * infrastructure for materialization jobs (rollup/transform evolution, async materialized
 * view refresh).
 *
 * <p><b>Backpressure.</b> At most {@code maxInFlightBulks} bulk requests are outstanding at
 * any time. When the limit is reached, {@link #feed} blocks on the semaphore until a bulk
 * completes — the producing stage's response-handler thread stalls, which in turn slows the
 * upstream Arrow stream. Crude but effective for a scheduled-job consumer; a fully
 * non-blocking handshake needs producer-side pause/resume and is follow-up work.
 *
 * <p><b>Document identity.</b> When {@code keyColumns} is non-empty, each document's
 * {@code _id} is a fixed-size hash of those column values (matching the rollup jobs'
 * deterministic-id approach), so re-running the same materialization is idempotent
 * per-document. Otherwise ids are auto-generated.
 *
 * <p><b>Lifecycle.</b> The engine calls {@link #close} when the query reaches a terminal
 * state — that only stops further feeds. The owner must then call {@link #finish} on query
 * success (flushes the tail buffer and completes when every in-flight bulk has responded)
 * or {@link #abort} on query failure (drops buffered docs; in-flight bulks are left to
 * complete and release their permits harmlessly).
 *
 * <p>Thread safety: {@code feed} may be called concurrently by shard/reduce handlers; all
 * mutation is serialized on {@code this}. Bulk responses arrive on transport threads and
 * only touch counters/completion under the same monitor.
 *
 * @opensearch.internal
 */
public class StreamingIndexSink implements ExchangeSink, ExchangeSource, StateSpecConsumer {

    private static final Logger logger = LogManager.getLogger(StreamingIndexSink.class);

    /** Default number of documents per bulk request. */
    public static final int DEFAULT_MAX_DOCS_PER_BULK = 1_000;
    /** Default cap on concurrently outstanding bulk requests (the backpressure bound). */
    public static final int DEFAULT_MAX_IN_FLIGHT_BULKS = 4;
    /** How long a feed will wait for a bulk slot before failing the query. */
    private static final long BULK_SLOT_TIMEOUT_SECONDS = 60;

    private final Client client;
    private final String targetIndex;
    private final List<String> keyColumns;
    private final int maxDocsPerBulk;
    private final Semaphore inFlightBulks;

    // All fields below guarded by `this`.
    private final List<IndexRequest> pending = new ArrayList<>();
    private long rowsReceived;
    private long docsIndexed;
    private long bulksSent;
    private int outstandingBulks;
    private Exception failure;
    private boolean closed;
    private boolean finishing;
    private ActionListener<Stats> finishListener;

    /** Immutable snapshot of what the sink wrote. */
    public record Stats(long rowsReceived, long docsIndexed, long bulksSent) {
    }

    public StreamingIndexSink(Client client, String targetIndex, List<String> keyColumns) {
        this(client, targetIndex, keyColumns, DEFAULT_MAX_DOCS_PER_BULK, DEFAULT_MAX_IN_FLIGHT_BULKS);
    }

    public StreamingIndexSink(Client client, String targetIndex, List<String> keyColumns, int maxDocsPerBulk, int maxInFlightBulks) {
        if (maxDocsPerBulk <= 0) {
            throw new IllegalArgumentException("maxDocsPerBulk must be > 0, got " + maxDocsPerBulk);
        }
        if (maxInFlightBulks <= 0) {
            throw new IllegalArgumentException("maxInFlightBulks must be > 0, got " + maxInFlightBulks);
        }
        this.client = client;
        this.targetIndex = targetIndex;
        this.keyColumns = keyColumns == null ? List.of() : List.copyOf(keyColumns);
        this.maxDocsPerBulk = maxDocsPerBulk;
        this.inFlightBulks = new Semaphore(maxInFlightBulks);
    }

    // ─── StateSpecConsumer (materialized-view refresh) ───────────────────

    /**
     * Provisions the view index from the backend-derived state spec, strictly before
     * the first batch is fed. The spec is the single source of truth: {@code
     * state_columns} become the index mappings, and the whole document is stored as
     * {@code index.parquet.mv.spec} so the parquet PartialReduce merge folds exactly
     * the layout the plan writes. No-op when the index already exists (delta refresh).
     */
    @Override
    @SuppressWarnings("unchecked")
    public void onStateSpec(String describeJson) {
        Map<String, Object> spec = XContentHelper.convertToMap(new BytesArray(describeJson), false, MediaTypeRegistry.JSON).v2();
        List<Map<String, Object>> stateColumns = (List<Map<String, Object>>) spec.get("state_columns");
        if (stateColumns == null || stateColumns.isEmpty()) {
            throw new IllegalStateException("state spec for [" + targetIndex + "] carries no state columns: " + describeJson);
        }
        Map<String, Object> properties = new java.util.LinkedHashMap<>();
        for (Map<String, Object> column : stateColumns) {
            String name = (String) column.get("name");
            String arrowType = (String) column.get("type");
            String mappingType = mappingTypeFor(arrowType, name);
            if ("binary".equals(mappingType)) {
                // Opaque accumulator states (HLL sketches etc.): binary fields derive
                // neither source nor doc values unless explicitly enabled.
                properties.put(name, Map.of("type", "binary", "doc_values", true, "store", true));
            } else {
                properties.put(name, Map.of("type", mappingType));
            }
        }
        // Parquet-only storage cannot back the _field_names metadata field (needs
        // full-text capability); the view never queries field existence anyway.
        // _source is derived (index.derived_source.enabled): reconstructed from the
        // columns at read time, so it can never go stale when the aggregating merge
        // folds rows — the folded row's content lives only in its columns.
        Map<String, Object> mapping = Map.of("_field_names", Map.of("enabled", false), "properties", properties);

        Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            // Lucene secondary is required (metadata mappers demand full-text capability);
            // the composite merge runs in MV mode: parquet folds states via the
            // PartialReduce merge and the lucene secondary is rebuilt 1:1 from the
            // folded output, so cross-format row parity always holds.
            .putList("index.composite.secondary_data_formats", "lucene")
            .put("index.parquet.mv.spec", describeJson)
            .build();
        try {
            CreateIndexResponse response = client.admin()
                .indices()
                .create(new CreateIndexRequest(targetIndex).settings(settings).mapping(mapping))
                .actionGet(TimeUnit.SECONDS.toMillis(30));
            if (response.isAcknowledged() == false) {
                throw new IllegalStateException("view index [" + targetIndex + "] creation was not acknowledged");
            }
            logger.info("[state-sink] created view index [{}] with {} state columns", targetIndex, stateColumns.size());
        } catch (org.opensearch.ResourceAlreadyExistsException e) {
            // Delta refresh: the view exists with the spec from its first refresh. The
            // stored spec must still describe what this plan writes — an engine upgrade
            // that changed a state layout, or any schema drift, makes appended states
            // unfoldable against the existing segments. Failing here leaves the refresh
            // intent set, so the next run rebuilds the view under the new layout.
            validateExistingSpec(describeJson);
        }
    }

    /**
     * Compares the live view's stored {@code index.parquet.mv.spec} against the spec
     * this refresh derived. State columns and engine version must match exactly;
     * anything else risks folding incompatible states.
     */
    private void validateExistingSpec(String describeJson) {
        String stored;
        try {
            stored = client.admin()
                .indices()
                .getSettings(new org.opensearch.action.admin.indices.settings.get.GetSettingsRequest().indices(targetIndex))
                .actionGet(TimeUnit.SECONDS.toMillis(30))
                .getSetting(targetIndex, "index.parquet.mv.spec");
        } catch (Exception e) {
            throw new IllegalStateException("view [" + targetIndex + "]: stored spec unreadable; refusing to append states", e);
        }
        if (stored == null || stored.isEmpty()) {
            throw new IllegalStateException("view [" + targetIndex + "] exists without a spec; refusing to append states");
        }
        Map<String, Object> storedSpec = XContentHelper.convertToMap(new BytesArray(stored), false, MediaTypeRegistry.JSON).v2();
        Map<String, Object> freshSpec = XContentHelper.convertToMap(new BytesArray(describeJson), false, MediaTypeRegistry.JSON).v2();
        if (java.util.Objects.equals(storedSpec.get("engine"), freshSpec.get("engine")) == false) {
            throw new IllegalStateException(
                "view ["
                    + targetIndex
                    + "] was written by engine "
                    + storedSpec.get("engine")
                    + " but this refresh runs "
                    + freshSpec.get("engine")
                    + "; state layouts may differ — the view will be rebuilt on the next run"
            );
        }
        if (java.util.Objects.equals(storedSpec.get("state_columns"), freshSpec.get("state_columns")) == false) {
            throw new IllegalStateException(
                "view ["
                    + targetIndex
                    + "] state schema drifted from its definition's current plan; the view will be rebuilt on the next run"
            );
        }
        logger.debug("[state-sink] view index [{}] spec validated for delta append", targetIndex);
    }

    /**
     * Maps an Arrow state-column type (DataType display form from the describe JSON) to
     * an index mapping type. Bounded transport glue, not aggregation logic: state
     * values round-trip through documents, and the merge/read plans cast back to the
     * exact accumulator state types.
     */
    private static String mappingTypeFor(String arrowType, String column) {
        if (arrowType.startsWith("Utf8") || arrowType.startsWith("LargeUtf8")) {
            return "keyword";
        }
        if (arrowType.startsWith("Int") || arrowType.startsWith("UInt")) {
            return "long";
        }
        if (arrowType.startsWith("Float") || arrowType.startsWith("Decimal")) {
            return "double";
        }
        if (arrowType.startsWith("Timestamp") || arrowType.startsWith("Date")) {
            return "date";
        }
        if (arrowType.startsWith("Boolean")) {
            return "boolean";
        }
        if (arrowType.startsWith("Binary") || arrowType.startsWith("LargeBinary") || arrowType.startsWith("FixedSizeBinary")) {
            return "binary";
        }
        throw new IllegalStateException("no mapping type for state column [" + column + "] of Arrow type [" + arrowType + "]");
    }

    // ─── ExchangeSink ────────────────────────────────────────────────────

    @Override
    public void feed(VectorSchemaRoot batch) {
        // Ownership contract: on throw the caller still owns the batch, so only close it
        // after all reads have succeeded (reading does not take ownership).
        synchronized (this) {
            if (failure != null) {
                // Fail the producing stage fast — the query terminal then surfaces the bulk failure.
                throw new IllegalStateException("StreamingIndexSink already failed writing to [" + targetIndex + "]", failure);
            }
            if (closed) {
                batch.close();
                return;
            }
            int rowCount = batch.getRowCount();
            List<IndexRequest> converted = new ArrayList<>(rowCount);
            for (int row = 0; row < rowCount; row++) {
                Map<String, Object> source = ArrowValues.toSourceMap(batch, row);
                IndexRequest request = new IndexRequest(targetIndex).source(source);
                if (keyColumns.isEmpty() == false) {
                    request.id(deterministicId(source));
                }
                converted.add(request);
            }
            // Reads complete — take ownership and release the Arrow buffers immediately.
            batch.close();
            rowsReceived += rowCount;
            pending.addAll(converted);
        }
        // Dispatch OUTSIDE the monitor: acquiring a bulk slot may block (that block is the
        // backpressure), and bulk-response handlers need the monitor to account completions.
        // Blocking while holding it would deadlock permit release (found the hard way at
        // multi-million-bucket cardinality).
        drainAndDispatch();
    }

    /** Sends full bulks while permits allow; the semaphore wait happens monitor-free. */
    private void drainAndDispatch() {
        while (true) {
            List<IndexRequest> slice;
            synchronized (this) {
                if (failure != null || pending.size() < maxDocsPerBulk) {
                    return;
                }
                slice = drainPending(maxDocsPerBulk);
            }
            if (!acquireBulkSlot()) {
                return;
            }
            sendBulk(slice);
        }
    }

    /** Blocks (bounded) for a bulk slot. On timeout/interrupt records the failure. */
    private boolean acquireBulkSlot() {
        try {
            if (inFlightBulks.tryAcquire(BULK_SLOT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                return true;
            }
            synchronized (this) {
                failure = new IllegalStateException(
                    "timed out after " + BULK_SLOT_TIMEOUT_SECONDS + "s waiting for a bulk slot writing to [" + targetIndex + "]"
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            synchronized (this) {
                failure = e;
            }
        }
        return false;
    }

    /** Caller must have acquired a bulk slot. */
    private void sendBulk(List<IndexRequest> docs) {
        BulkRequest bulk = new BulkRequest();
        for (IndexRequest doc : docs) {
            bulk.add(doc);
        }
        synchronized (this) {
            outstandingBulks++;
            bulksSent++;
        }
        client.bulk(bulk, ActionListener.wrap(this::onBulkResponse, this::onBulkFailure));
    }

    @Override
    public synchronized void close() {
        // Engine-terminal signal: no more feeds. Deliberately does NOT drop pending docs or
        // touch in-flight bulks — finish()/abort() own completion, and either may run after
        // this (QueryExecution fires the listener before close()).
        closed = true;
    }

    // ─── ExchangeSource (root gather read view) ──────────────────────────

    @Override
    public synchronized Iterable<VectorSchemaRoot> readResult() {
        // Batches are forwarded, never retained. The engine's terminal listener sees an
        // empty result; the owner reads write stats via finish().
        return List.of();
    }

    @Override
    public synchronized long getRowCount() {
        return rowsReceived;
    }

    // ─── Owner lifecycle ─────────────────────────────────────────────────

    /**
     * Flushes any buffered tail and completes {@code listener} once every in-flight bulk has
     * responded — with {@link Stats} if all writes succeeded, or the first write failure.
     * Call exactly once, after the engine reports query success.
     */
    public void finish(ActionListener<Stats> listener) {
        synchronized (this) {
            if (finishing) {
                listener.onFailure(new IllegalStateException("finish() already called for sink on [" + targetIndex + "]"));
                return;
            }
            finishing = true;
            finishListener = listener;
        }
        // Flush everything left (full bulks + partial tail), slot waits monitor-free.
        drainAndDispatch();
        List<IndexRequest> tail;
        synchronized (this) {
            tail = pending.isEmpty() ? List.of() : drainPending(pending.size());
        }
        if (!tail.isEmpty() && acquireBulkSlot()) {
            sendBulk(tail);
        }
        synchronized (this) {
            maybeCompleteFinish();
        }
    }

    /**
     * Drops buffered documents after a query failure. In-flight bulks run to completion and
     * release their permits; their results are ignored. Idempotent.
     */
    public synchronized void abort() {
        closed = true;
        pending.clear();
    }

    // ─── Internals ───────────────────────────────────────────────────────

    private List<IndexRequest> drainPending(int count) {
        List<IndexRequest> slice = new ArrayList<>(pending.subList(0, count));
        pending.subList(0, count).clear();
        return slice;
    }

    private void onBulkResponse(BulkResponse response) {
        synchronized (this) {
            long succeeded = 0;
            Exception firstItemFailure = null;
            for (BulkItemResponse item : response.getItems()) {
                if (item.isFailed()) {
                    if (firstItemFailure == null) {
                        firstItemFailure = new IllegalStateException(
                            "bulk item failed writing to [" + targetIndex + "]: " + item.getFailureMessage()
                        );
                    }
                } else {
                    succeeded++;
                }
            }
            docsIndexed += succeeded;
            if (firstItemFailure != null && failure == null) {
                failure = firstItemFailure;
            }
            releaseAndMaybeComplete();
        }
    }

    private void onBulkFailure(Exception e) {
        synchronized (this) {
            logger.warn("StreamingIndexSink bulk write to [{}] failed", targetIndex, e);
            if (failure == null) {
                failure = e;
            }
            releaseAndMaybeComplete();
        }
    }

    /** Caller must hold the monitor. */
    private void releaseAndMaybeComplete() {
        outstandingBulks--;
        inFlightBulks.release();
        maybeCompleteFinish();
    }

    /** Caller must hold the monitor. */
    private void maybeCompleteFinish() {
        if (finishing == false || finishListener == null || outstandingBulks > 0) {
            return;
        }
        ActionListener<Stats> listener = finishListener;
        finishListener = null;
        if (failure != null) {
            listener.onFailure(failure);
        } else {
            listener.onResponse(new Stats(rowsReceived, docsIndexed, bulksSent));
        }
    }

    /**
     * Fixed-size deterministic id from the key column values — same shape as the rollup
     * jobs' {@code hashToFixedSize(jobId + bucketKey)} so re-materialization overwrites
     * rather than duplicates. Missing key values hash as the literal "\0null\0" marker.
     */
    private String deterministicId(Map<String, Object> source) {
        StringBuilder sb = new StringBuilder();
        for (String column : keyColumns) {
            Object value = source.get(column);
            sb.append('\u0000').append(value == null ? "null" : value.toString()).append('\u0000');
        }
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        byte[] hash = digest.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
        // 20 bytes → 27-char url-safe id, same budget as UUID-based auto ids.
        byte[] truncated = new byte[20];
        System.arraycopy(hash, 0, truncated, 0, truncated.length);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(truncated);
    }
}
