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
            NativeBridge.createGlobalRuntime(1L * 1024 * 1024 * 1024, 0L, spillDir.toString(), 2L * 1024 * 1024 * 1024)
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
        var pb = new SubstraitPlanBuilder(cols, types);

        // CSV output
        String csvPath = System.getProperty("user.dir") + "/perf_metrics.csv";
        csvWriter = new java.io.PrintWriter(new java.io.FileWriter(csvPath));
        csvWriter.println("query\tmatch\tsql_ms\tidx_ms\tspeedup\tsql_scan_rows\tidx_scan_rows\tsql_bytes_scanned\tidx_bytes_scanned\tsql_compute\tidx_compute\tidx_index_time\tidx_parquet_time\tidx_ffm_calls\tidx_rows_matched\tidx_rg_processed\tidx_rg_skipped\tidx_rows_pruned_page\tidx_row_granular\tidx_block_granular\tidx_prefetch_wait_time\tidx_prefetch_wait_count\tidx_coalesce_time\tidx_batches_pre_coalesce");

        // ── Run each query via indexed path AND SQL baseline, compare results ──

        // Q1: selective collector + predicate
        compare(reader, runtime, pb, "Q1",
            pb.buildAndCollectorPredicateCount("test_table", "client_ip", "14.175.54.83", "elb_status_code", "eq", 200),
            "SELECT COUNT(*) FROM test_table WHERE client_ip = '14.175.54.83' AND elb_status_code = 200");

        // Q2: 7% collector × 50% predicate
        compare(reader, runtime, pb, "Q2",
            pb.buildAndCollectorPredicateCount("test_table", "http_version", "HTTP/2.0", "sent_bytes", "gt", 500_000_000),
            "SELECT COUNT(*) FROM test_table WHERE http_version = 'HTTP/2.0' AND sent_bytes > 500000000");

        // Q5: collector × predicate
        compare(reader, runtime, pb, "Q5",
            pb.buildAndCollectorPredicateCount("test_table", "http_version", "HTTP/1.1", "elb_status_code", "eq", 200),
            "SELECT COUNT(*) FROM test_table WHERE http_version = 'HTTP/1.1' AND elb_status_code = 200");

        // Q7: collector only
        compare(reader, runtime, pb, "Q7",
            pb.buildCollectorOnlyCount("test_table", "http_version", "HTTP/1.1"),
            "SELECT COUNT(*) FROM test_table WHERE http_version = 'HTTP/1.1'");

        // Q9: GROUP BY collector-only
        compare(reader, runtime, pb, "Q9",
            pb.buildCollectorOnlyGroupBy("test_table", "http_version", "HTTP/1.1", "elb_status_code"),
            "SELECT elb_status_code, COUNT(*) AS cnt FROM test_table WHERE http_version = 'HTTP/1.1' GROUP BY elb_status_code");

        // Q10: GROUP BY collector + predicate
        compare(reader, runtime, pb, "Q10",
            pb.buildAndCollectorPredicateGroupBy("test_table", "http_version", "HTTP/1.1", "elb_status_code", "eq", 200, "backend_status_code"),
            "SELECT backend_status_code, COUNT(*) AS cnt FROM test_table WHERE http_version = 'HTTP/1.1' AND elb_status_code = 200 GROUP BY backend_status_code");

        // Q11: Tree path — two collectors
        compare(reader, runtime, pb, "Q11",
            pb.buildTwoCollectorAndCount("test_table", "http_version", "HTTP/1.0", "client_ip", "14.175.54.83"),
            "SELECT COUNT(*) FROM test_table WHERE http_version = 'HTTP/1.0' AND client_ip = '14.175.54.83'");

        // Q12: BitmapTree — (Collector OR Predicate) AND (Collector OR Predicate)
        compare(reader, runtime, pb, "Q12",
            pb.buildMixedOrCount("test_table",
                "http_version", "HTTP/1.0", "elb_status_code", "eq", 200,
                "client_ip", "14.175.54.83", "elb_status_code", "eq", 404),
            "SELECT COUNT(*) FROM test_table WHERE (http_version = 'HTTP/1.0' OR elb_status_code = 200) AND (client_ip = '14.175.54.83' OR elb_status_code = 404)");

        // ── GROUP BY + SUM queries ──

        // Q13: GROUP BY with SUM — collector only
        compare(reader, runtime, pb, "Q13",
            pb.buildCollectorOnlyGroupBySum("test_table", "http_version", "HTTP/1.1",
                "elb_status_code", "sent_bytes"),
            "SELECT elb_status_code, SUM(sent_bytes) AS total FROM test_table WHERE http_version = 'HTTP/1.1' GROUP BY elb_status_code");

        // Q14: GROUP BY with COUNT + SUM — collector + predicate
        compare(reader, runtime, pb, "Q14",
            pb.buildCollectorPredicateGroupByCountSum("test_table", "http_version", "HTTP/1.1",
                "elb_status_code", "eq", 200, "backend_status_code", "sent_bytes"),
            "SELECT backend_status_code, COUNT(*) AS cnt, SUM(sent_bytes) AS total FROM test_table WHERE http_version = 'HTTP/1.1' AND elb_status_code = 200 GROUP BY backend_status_code");

        // Q15: GROUP BY selective collector — few groups expected
        compare(reader, runtime, pb, "Q15",
            pb.buildCollectorOnlyGroupBy("test_table", "client_ip", "14.175.54.83",
                "elb_status_code"),
            "SELECT elb_status_code, COUNT(*) AS cnt FROM test_table WHERE client_ip = '14.175.54.83' GROUP BY elb_status_code");

        // ── Prefetch-stress queries (multiple collectors + status code predicates) ──

        // Q16: 3 collectors AND status code predicate. Tree path with
        // cheap residual. `http_version='HTTP/1.1' AND http_version='HTTP/2.0'
        // AND client_ip='...' AND elb_status_code=404`
        compare(reader, runtime, pb, "Q16",
            pb.buildThreeCollectorAndPredicateCount("test_table",
                "http_version", "HTTP/1.1",
                "http_version", "HTTP/2.0",
                "client_ip", "14.175.54.83",
                "elb_status_code", "eq", 404),
            "SELECT COUNT(*) FROM test_table WHERE http_version = 'HTTP/1.1' AND http_version = 'HTTP/2.0' AND client_ip = '14.175.54.83' AND elb_status_code = 404");

        // Q17: Top-level OR of two AND-groups, each collector+status code.
        // `(http_version='HTTP/1.1' AND elb=200) OR (http_version='HTTP/2.0' AND elb=504)`
        compare(reader, runtime, pb, "Q17",
            pb.buildTwoGroupOrCount("test_table",
                "http_version", "HTTP/1.1", "elb_status_code", "eq", 200,
                "http_version", "HTTP/2.0", "elb_status_code", "eq", 504),
            "SELECT COUNT(*) FROM test_table WHERE (http_version = 'HTTP/1.1' AND elb_status_code = 200) OR (http_version = 'HTTP/2.0' AND elb_status_code = 504)");

        // Q18: Three collectors AND'd, all fat. Prefetch stress, no
        // predicate shortcut.
        compare(reader, runtime, pb, "Q18",
            pb.buildThreeCollectorAndCount("test_table",
                "http_version", "HTTP/1.1",
                "http_version", "HTTP/2.0",
                "http_version", "HTTP/1.0"),
            "SELECT COUNT(*) FROM test_table WHERE http_version = 'HTTP/1.1' AND http_version = 'HTTP/2.0' AND http_version = 'HTTP/1.0'");

        // Q19: Three collectors OR'd covering all HTTP versions (union
        // saturates the universe). Tests OR-recovery materialising all bitmaps.
        compare(reader, runtime, pb, "Q19",
            pb.buildThreeCollectorOrCount("test_table",
                "http_version", "HTTP/1.1",
                "http_version", "HTTP/1.0",
                "http_version", "HTTP/2.0"),
            "SELECT COUNT(*) FROM test_table WHERE http_version = 'HTTP/1.1' OR http_version = 'HTTP/1.0' OR http_version = 'HTTP/2.0'");

        // Q20: Depth-4 tree, 4 collectors in two OR-groups.
        // `(c1 OR c2) AND (c3 OR c4)` — Q12 shape but all text collectors.
        compare(reader, runtime, pb, "Q20",
            pb.buildFourCollectorTreeCount("test_table",
                "client_ip", "14.175.54.83",   // selective
                "http_version", "HTTP/1.1",     // fat
                "client_ip", "14.175.54.83",
                "http_version", "HTTP/2.0"),    // moderate
            "SELECT COUNT(*) FROM test_table WHERE (client_ip = '14.175.54.83' OR http_version = 'HTTP/1.1') AND (client_ip = '14.175.54.83' OR http_version = 'HTTP/2.0')");

        // Q21: Deep + wide — 6 collectors, 3 status-code predicates,
        // depth 4. `(c1 OR c2) AND ((c3 AND p1) OR (c4 AND p2)) AND (c5 OR (c6 AND p3))`
        compare(reader, runtime, pb, "Q21",
            pb.buildDeepTreeCount("test_table",
                new String[]{
                    "http_version", "http_version",
                    "client_ip", "http_version",
                    "http_version", "client_ip"},
                new String[]{
                    "HTTP/1.1", "HTTP/2.0",
                    "14.175.54.83", "HTTP/1.0",
                    "HTTP/1.1", "14.175.54.83"},
                new String[]{"elb_status_code", "elb_status_code", "elb_status_code"},
                new String[]{"eq", "eq", "eq"},
                new int[]{200, 404, 504}),
            "SELECT COUNT(*) FROM test_table WHERE (http_version='HTTP/1.1' OR http_version='HTTP/2.0') " +
                "AND ((client_ip='14.175.54.83' AND elb_status_code=200) OR (http_version='HTTP/1.0' AND elb_status_code=404)) " +
                "AND (http_version='HTTP/1.1' OR (client_ip='14.175.54.83' AND elb_status_code=504))");

        // Q22: Extra-wide tree — 7 collectors, 3 status-code predicates,
        // depth 5. `((c1 AND c2) OR (c3 AND p1)) AND ((c4 OR c5) AND (c6 OR p2)) AND (c7 AND p3)`
        compare(reader, runtime, pb, "Q22",
            pb.buildExtraDeepTreeCount("test_table",
                new String[]{
                    "http_version", "client_ip",
                    "http_version", "http_version",
                    "http_version", "client_ip",
                    "http_version"},
                new String[]{
                    "HTTP/1.1", "14.175.54.83",
                    "HTTP/2.0", "HTTP/1.0",
                    "HTTP/1.1", "14.175.54.83",
                    "HTTP/2.0"},
                new String[]{"elb_status_code", "elb_status_code", "elb_status_code"},
                new String[]{"eq", "eq", "eq"},
                new int[]{404, 200, 504}),
            "SELECT COUNT(*) FROM test_table WHERE ((http_version='HTTP/1.1' AND client_ip='14.175.54.83') OR (http_version='HTTP/2.0' AND elb_status_code=404)) " +
                "AND ((http_version='HTTP/1.0' OR http_version='HTTP/1.1') AND (client_ip='14.175.54.83' OR elb_status_code=200)) " +
                "AND (http_version='HTTP/2.0' AND elb_status_code=504)");

        // ── Wildcard (Lucene WildcardQuery) — slow posting-list enumeration ──

        // Q23: Wildcard single collector matching everything starting with
        // HTTP/. Expected slow because WildcardQuery enumerates terms and
        // unions postings; narrow use case, tests FFM throughput with a
        // fat wildcard.
        compare(reader, runtime, pb, "Q23",
            pb.buildCollectorOnlyCount("test_table", "http_version", "HTTP/*"),
            "SELECT COUNT(*) FROM test_table WHERE http_version LIKE 'HTTP/%'");

        // Q24: Wildcard AND with narrow predicate. Tests whether a
        // selective residual speeds up the wildcard scan or not.
        compare(reader, runtime, pb, "Q24",
            pb.buildAndCollectorPredicateCount("test_table",
                "http_version", "HTTP/*", "elb_status_code", "eq", 504),
            "SELECT COUNT(*) FROM test_table WHERE http_version LIKE 'HTTP/%' AND elb_status_code = 504");

        // Q25: Wildcard on IP — 14.175.* matches multiple IPs, slow.
        compare(reader, runtime, pb, "Q25",
            pb.buildCollectorOnlyCount("test_table", "client_ip", "14.175.*"),
            "SELECT COUNT(*) FROM test_table WHERE client_ip LIKE '14.175.%'");

        // Q26: Wildcard OR term — mixed in tree path.
        compare(reader, runtime, pb, "Q26",
            pb.buildTwoCollectorAndCount("test_table",
                "http_version", "HTTP/1.*",   // wildcard (fat, HTTP/1.0 + HTTP/1.1)
                "client_ip", "14.175.54.83"),  // selective term
            "SELECT COUNT(*) FROM test_table WHERE http_version LIKE 'HTTP/1.%' AND client_ip = '14.175.54.83'");

        csvWriter.close();
        System.out.println("\nCSV written to: " + csvPath);

        reader.close();
        factory.closeReader();
        FilterTreeCallbacks.setRegistries(null, null);
        runtime.close();
    }

    static java.io.PrintWriter csvWriter;

    /** Run indexed plan and SQL baseline, compare results and timing. */
    static void compare(ReaderHandle reader, NativeRuntimeHandle runtime,
                        SubstraitPlanBuilder pb, String label,
                        byte[] indexedPlan, String sql) throws Exception {
        System.out.println("\n=== " + label + ": " + sql + " ===");

        // SQL baseline — warmup + 3 timed runs
        byte[] sqlPlan = NativeBridge.sqlToSubstrait(
            reader.getPointer(), "test_table", sql, runtime.getPointer());
        runAndPrint(reader, runtime, sqlPlan); // warmup
        long sqlTotal = 0;
        String sqlResult = null;
        for (int r = 0; r < 3; r++) {
            long t0 = System.nanoTime();
            sqlResult = runAndPrint(reader, runtime, sqlPlan);
            sqlTotal += System.nanoTime() - t0;
        }
        double sqlMs = sqlTotal / 3.0 / 1e6;
        String sqlExplain = readExplain();

        // Indexed — warmup + 3 timed runs
        runAndPrint(reader, runtime, indexedPlan); // warmup
        long idxTotal = 0;
        String idxResult = null;
        for (int r = 0; r < 3; r++) {
            long t0 = System.nanoTime();
            idxResult = runAndPrint(reader, runtime, indexedPlan);
            idxTotal += System.nanoTime() - t0;
        }
        double idxMs = idxTotal / 3.0 / 1e6;
        String idxExplain = readExplain();

        boolean match = normalizeResult(sqlResult).equals(normalizeResult(idxResult));
        double speedup = sqlMs / idxMs;

        System.out.printf("  SQL:     %.1f ms  %s%n", sqlMs, sqlResult.length() > 80 ? sqlResult.substring(0, 80) + "..." : sqlResult);
        System.out.printf("  Indexed: %.1f ms  %s%n", idxMs, idxResult.length() > 80 ? idxResult.substring(0, 80) + "..." : idxResult);
        System.out.printf("  %s  speedup=%.1fx%n", match ? "✓ MATCH" : "✗ MISMATCH", speedup);

        // Print both explains
        System.out.println("  [SQL plan]     " + sqlExplain.replace("\n", "\n                  "));
        System.out.println("  [Indexed plan] " + idxExplain.replace("\n", "\n                  "));

        // TSV — extract key metrics from explain-analyze into columns
        csvWriter.printf("%s\t%s\t%.1f\t%.1f\t%.1f\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s%n",
            label, match ? "✓" : "✗",
            sqlMs, idxMs, speedup,
            em(sqlExplain, "output_rows", "DataSource"),
            em(idxExplain, "output_rows", "QueryShard"),
            em(sqlExplain, "bytes_scanned"),
            em(idxExplain, "bytes_scanned"),
            em(sqlExplain, "elapsed_compute", "DataSource"),
            em(idxExplain, "elapsed_compute", "QueryShard"),
            em(idxExplain, "index_query_time"),
            em(idxExplain, "parquet_read_time"),
            em(idxExplain, "ffm_collector_calls"),
            em(idxExplain, "rows_matched"),
            em(idxExplain, "row_groups_processed"),
            em(idxExplain, "row_groups_skipped"),
            em(idxExplain, "rows_pruned_by_page_index"),
            em(idxExplain, "min_skip_run_row_granular"),
            em(idxExplain, "min_skip_run_block_granular"),
            em(idxExplain, "prefetch_wait_time"),
            em(idxExplain, "prefetch_wait_count"),
            em(idxExplain, "coalesce_time"),
            em(idxExplain, "batches_pre_coalesce"));
        csvWriter.flush();
    }

    /** Extract a metric value from explain-analyze text. */
    static String em(String explain, String metric) {
        return em(explain, metric, null);
    }
    static String em(String explain, String metric, String nearNode) {
        if (explain.isEmpty()) return "";
        // If nearNode specified, find the section containing that node first
        String section = explain;
        if (nearNode != null) {
            int idx = explain.indexOf(nearNode);
            if (idx >= 0) section = explain.substring(idx);
        }
        var m = java.util.regex.Pattern.compile("(?<![a-z_])" + metric + "=([^,\\]|]+)").matcher(section);
        return m.find() ? m.group(1).strip() : "";
    }

    static String readExplain() {
        try { return Files.readString(Path.of("/tmp/_df_explain_analyze.txt")).strip(); }
        catch (Exception e) { return ""; }
    }

    /** Normalize result string for comparison — sort semicolon-separated groups. */
    static String normalizeResult(String s) {
        String[] parts = s.split("; ");
        java.util.Arrays.sort(parts);
        return String.join("; ", parts);
    }

    /** Execute a plan and return a string representation of all result rows. */
    static String runAndPrint(ReaderHandle reader, NativeRuntimeHandle runtime, byte[] plan) throws Exception {
        long streamPtr = call(l -> NativeBridge.executeQueryAsync(
            reader.getPointer(), "test_table", plan, runtime.getPointer(), 0L, l));
        try (StreamHandle stream = new StreamHandle(streamPtr, runtime);
             RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
             CDataDictionaryProvider dp = new CDataDictionaryProvider()) {
            long schemaAddr = call(l -> NativeBridge.streamGetSchema(stream.getPointer(), l));
            Schema s = new Schema(importField(alloc, ArrowSchema.wrap(schemaAddr), dp).getChildren(), null);
            VectorSchemaRoot root = VectorSchemaRoot.create(s, alloc);
            StringBuilder sb = new StringBuilder();
            while (true) {
                long arr = call(l -> NativeBridge.streamNext(runtime.getPointer(), stream.getPointer(), l));
                if (arr == 0) break;
                Data.importIntoVectorSchemaRoot(alloc, ArrowArray.wrap(arr), root, dp);
                for (int r = 0; r < root.getRowCount(); r++) {
                    if (sb.length() > 0) sb.append("; ");
                    for (int c = 0; c < root.getFieldVectors().size(); c++) {
                        if (c > 0) sb.append("=");
                        sb.append(root.getFieldVectors().get(c).getObject(r));
                    }
                }
            }
            root.close();
            return sb.toString();
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
