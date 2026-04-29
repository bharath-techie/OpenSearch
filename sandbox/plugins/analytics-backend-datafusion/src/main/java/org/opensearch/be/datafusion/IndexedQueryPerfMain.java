/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.be.datafusion.indexfilter.CollectorRegistry;
import org.opensearch.be.datafusion.indexfilter.FilterProviderRegistry;
import org.opensearch.be.datafusion.indexfilter.FilterTreeCallbacks;
import org.opensearch.be.datafusion.indexfilter.LuceneTermQueryFactory;
import org.opensearch.be.datafusion.indexfilter.SubstraitPlanBuilder;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.arrow.c.Data.importField;

import io.substrait.proto.Type;

/** Standalone e2e perf test. No test framework. */
public class IndexedQueryPerfMain {

    public static void main(String[] args) throws Exception {
        String dir = args.length > 0 ? args[0]
            : "/Users/abandeji/Public/work-dump/experiments/data/nodes/0/indices/ATTb8ViJT0mrLoeWpkEtrA/0";
        Path shardDir = Path.of(dir);

        NativeBridge.initTokioRuntimeManager(4);
        Path spillDir = Files.createTempDirectory("perf-spill");
        NativeRuntimeHandle runtime = new NativeRuntimeHandle(
            NativeBridge.createGlobalRuntime(2L * 1024 * 1024 * 1024, 0L, spillDir.toString(), 1L * 1024 * 1024 * 1024)
        );

        CollectorRegistry collectors = new CollectorRegistry();
        FilterProviderRegistry providers = new FilterProviderRegistry(collectors);
        LuceneTermQueryFactory factory = new LuceneTermQueryFactory(shardDir.resolve("index"));
        providers.setFactory(factory);
        FilterTreeCallbacks.setRegistries(providers, collectors);

        ReaderHandle reader = new ReaderHandle(shardDir.resolve("parquet").toString(),
            new String[]{"generation-1.parquet", "generation-2.parquet"});

        List<String> cols = List.of(
            "backend_ip","backend_port","backend_processing_time","backend_status_code",
            "client_ip","client_port","connection_time","destination_ip","destination_port",
            "elb_status_code","http_port","http_version","matched_rule_priority",
            "received_bytes","request_creation_time","request_processing_time",
            "response_processing_time","sent_bytes","target_ip","target_port",
            "target_processing_time","target_status_code","timestamp","___row_id","_id");
        List<Type> types = List.of(
            SubstraitPlanBuilder.binaryType(), SubstraitPlanBuilder.i32Type(),
            SubstraitPlanBuilder.fp32Type(), SubstraitPlanBuilder.i32Type(),
            SubstraitPlanBuilder.binaryType(), SubstraitPlanBuilder.i32Type(),
            SubstraitPlanBuilder.fp32Type(), SubstraitPlanBuilder.binaryType(),
            SubstraitPlanBuilder.i32Type(), SubstraitPlanBuilder.i32Type(),
            SubstraitPlanBuilder.i32Type(), SubstraitPlanBuilder.binaryType(),
            SubstraitPlanBuilder.i32Type(), SubstraitPlanBuilder.i32Type(),
            SubstraitPlanBuilder.i64Type(), SubstraitPlanBuilder.fp32Type(),
            SubstraitPlanBuilder.fp32Type(), SubstraitPlanBuilder.i32Type(),
            SubstraitPlanBuilder.binaryType(), SubstraitPlanBuilder.i32Type(),
            SubstraitPlanBuilder.fp32Type(), SubstraitPlanBuilder.i32Type(),
            SubstraitPlanBuilder.i64Type(), SubstraitPlanBuilder.i32Type(),
            SubstraitPlanBuilder.binaryType());

        byte[] plan = new SubstraitPlanBuilder(cols, types).buildAndCollectorPredicate(
            "test_table", "client_ip", "14.175.54.83", "elb_status_code", "eq", 200);

        System.out.println("=== Q1: AND(TermQuery(client_ip=14.175.54.83), elb_status_code=200) ===");
        System.out.println("  SingleCollector path, selective Collector + selective predicate");
        benchWithBaseline(reader, runtime, plan, null);

        byte[] plan2 = new SubstraitPlanBuilder(cols, types).buildAndCollectorPredicate(
            "test_table", "http_version", "HTTP/2.0", "sent_bytes", "gt", 500_000_000);
        System.out.println("\n=== Q2: AND(TermQuery(http_version=HTTP/2.0), sent_bytes > 500M) ===");
        System.out.println("  SingleCollector, ~7% Collector × ~50% predicate");
        benchWithBaseline(reader, runtime, plan2, null);

        // Q5: HEAVY — ~60% Collector × ~70% predicate
        byte[] plan5 = new SubstraitPlanBuilder(cols, types).buildAndCollectorPredicate(
            "test_table", "http_version", "HTTP/1.0", "elb_status_code", "eq", 200);
        System.out.println("\n=== Q5: AND(TermQuery(http_version=HTTP/1.0), elb_status_code=200) ===");
        System.out.println("  SingleCollector, ~60% Collector × ~70% predicate = ~6M rows");
        benchWithBaseline(reader, runtime, plan5, null);

        // Q7: HEAVY collector only — ~60% of 14M
        byte[] plan7 = new SubstraitPlanBuilder(cols, types).buildCollectorOnly(
            "test_table", "http_version", "HTTP/1.0");
        System.out.println("\n=== Q7: TermQuery(http_version=HTTP/1.0) only ===");
        System.out.println("  SingleCollector, ~60% Collector, ~8.5M rows");
        benchWithBaseline(reader, runtime, plan7, null);

        // Q9: GROUP BY on indexed path — filter via Lucene, aggregate in DataFusion
        byte[] plan9 = new SubstraitPlanBuilder(cols, types).buildCollectorOnlyGroupBy(
            "test_table", "http_version", "HTTP/1.0", "elb_status_code");
        System.out.println("\n=== Q9: SELECT elb_status_code, COUNT(*) WHERE http_version=HTTP/1.0 GROUP BY elb_status_code ===");
        System.out.println("  Indexed filter (Lucene) + aggregation (DataFusion), ~8.5M input → 8 groups");
        benchWithBaseline(reader, runtime, plan9, null);

        // Q10: GROUP BY with both Collector + predicate
        byte[] plan10 = new SubstraitPlanBuilder(cols, types).buildAndCollectorPredicateGroupBy(
            "test_table", "http_version", "HTTP/1.0", "elb_status_code", "eq", 200, "backend_status_code");
        System.out.println("\n=== Q10: SELECT backend_status_code, COUNT(*) WHERE http_version=HTTP/1.0 AND elb_status_code=200 GROUP BY backend_status_code ===");
        System.out.println("  Indexed filter + predicate + aggregation, ~6M input → 8 groups");
        benchWithBaseline(reader, runtime, plan10, null);

        reader.close();
        factory.closeReader();
        FilterTreeCallbacks.setRegistries(null, null);
        runtime.close();
    }

    // ── SQL baseline for correctness ──────────────────────────────

    static void benchWithBaseline(ReaderHandle reader, NativeRuntimeHandle runtime,
                                   byte[] plan, String baselineSql) throws Exception {
        // Indexed path
        long warm = run(reader, runtime, plan);
        System.out.println("  Warmup: " + warm + " rows");
        long totalRows = 0, totalNanos = 0;
        int runs = 3;
        for (int i = 1; i <= runs; i++) {
            long t0 = System.nanoTime();
            long rows = run(reader, runtime, plan);
            long ns = System.nanoTime() - t0;
            totalRows += rows;
            totalNanos += ns;
            System.out.printf("    indexed run %d: %d rows in %.1f ms%n", i, rows, ns / 1e6);
        }
        double avgMs = totalNanos / runs / 1e6;
        long avgRows = totalRows / runs;
        System.out.printf("  Indexed avg: %d rows in %.1f ms%n", avgRows, avgMs);

        // SQL baseline (COUNT(*) query — returns 1 row with the count)
        if (baselineSql != null) {
            long t0 = System.nanoTime();
            // Run the COUNT query and extract the scalar value
            long baselineCount = sqlScalar(reader, runtime, baselineSql);
            double baseMs = (System.nanoTime() - t0) / 1e6;
            System.out.printf("  SQL baseline: %d rows in %.1f ms%n", baselineCount, baseMs);
            if (baselineCount != avgRows) {
                System.out.printf("  *** MISMATCH: indexed=%d vs sql=%d ***%n", avgRows, baselineCount);
            } else {
                System.out.println("  ✓ Correctness: indexed == SQL baseline");
            }
        }
    }

    /** Run a COUNT(*) SQL query and return the scalar count value. */
    static long sqlScalar(ReaderHandle reader, NativeRuntimeHandle runtime, String sql) throws Exception {
        byte[] substrait = NativeBridge.sqlToSubstrait(
            reader.getPointer(), "test_table", sql, runtime.getPointer());
        long streamPtr = call(l -> NativeBridge.executeQueryAsync(
            reader.getPointer(), "test_table", substrait, runtime.getPointer(), 0L, l));
        try (StreamHandle stream = new StreamHandle(streamPtr, runtime);
             RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
             CDataDictionaryProvider dp = new CDataDictionaryProvider()) {
            long schemaAddr = call(l -> NativeBridge.streamGetSchema(stream.getPointer(), l));
            Schema s = new Schema(importField(alloc, ArrowSchema.wrap(schemaAddr), dp).getChildren(), null);
            VectorSchemaRoot root = VectorSchemaRoot.create(s, alloc);
            long result = 0;
            while (true) {
                long arr = call(l -> NativeBridge.streamNext(runtime.getPointer(), stream.getPointer(), l));
                if (arr == 0) break;
                Data.importIntoVectorSchemaRoot(alloc, ArrowArray.wrap(arr), root, dp);
                if (root.getRowCount() > 0) {
                    // First column of first row = the COUNT value
                    result = ((Number) root.getFieldVectors().get(0).getObject(0)).longValue();
                }
            }
            root.close();
            return result;
        }
    }

    static long run(ReaderHandle reader, NativeRuntimeHandle runtime, byte[] plan) throws Exception {
        long streamPtr = call(l -> NativeBridge.executeQueryAsync(
            reader.getPointer(), "test_table", plan, runtime.getPointer(), 0L, l));
        try (StreamHandle stream = new StreamHandle(streamPtr, runtime)) {
            long schemaAddr = call(l -> NativeBridge.streamGetSchema(stream.getPointer(), l));
            long total = 0;
            try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
                 CDataDictionaryProvider dp = new CDataDictionaryProvider()) {
                Schema s = new Schema(importField(alloc, ArrowSchema.wrap(schemaAddr), dp).getChildren(), null);
                VectorSchemaRoot root = VectorSchemaRoot.create(s, alloc);
                while (true) {
                    long arr = call(l -> NativeBridge.streamNext(runtime.getPointer(), stream.getPointer(), l));
                    if (arr == 0) break;
                    Data.importIntoVectorSchemaRoot(alloc, ArrowArray.wrap(arr), root, dp);
                    total += root.getRowCount();
                }
                root.close();
            }
            return total;
        }
    }

    static long call(java.util.function.Consumer<ActionListener<Long>> c) throws Exception {
        CompletableFuture<Long> f = new CompletableFuture<>();
        c.accept(new ActionListener<>() {
            @Override public void onResponse(Long v) { f.complete(v); }
            @Override public void onFailure(Exception e) { f.completeExceptionally(e); }
        });
        return f.get();
    }
}
