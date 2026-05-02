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

        // ── Page-pruning proof queries (___row_id is monotonic → tight page stats) ──

        // Q27: Collector + ___row_id < 10000. ~99% of pages pruned per RG.
        // Page-range splitting should call collector only for the first ~1-2 pages.
        compare(reader, runtime, pb, "Q27",
            pb.buildAndCollectorPredicateCount("test_table",
                "http_version", "HTTP/1.1", "___row_id", "lt", 10000),
            "SELECT COUNT(*) FROM test_table WHERE http_version = 'HTTP/1.1' AND ___row_id < 10000");

        // Q28: Collector + ___row_id range in the middle. Pages before and
        // after the range are pruned — tests gap skipping.
        compare(reader, runtime, pb, "Q28",
            pb.buildAndCollectorPredicateCount("test_table",
                "http_version", "HTTP/1.1", "___row_id", "gt", 500000),
            "SELECT COUNT(*) FROM test_table WHERE http_version = 'HTTP/1.1' AND ___row_id > 500000");

        // Q29: Selective collector + tight ___row_id range. Both sides
        // are selective — page pruning + collector narrowing compound.
        compare(reader, runtime, pb, "Q29",
            pb.buildAndCollectorPredicateCount("test_table",
                "client_ip", "14.175.54.83", "___row_id", "lt", 100000),
            "SELECT COUNT(*) FROM test_table WHERE client_ip = '14.175.54.83' AND ___row_id < 100000");

        // ── Expensive wildcard + page-pruning range queries ──

        // Q30: Expensive leading-wildcard collector + 99% page pruning.
        // `*14.175*` forces Lucene to enumerate all terms — expensive FFM call.
        // ___row_id < 10000 prunes 99.6% of pages.
        compare(reader, runtime, pb, "Q30",
            pb.buildAndCollectorPredicateCount("test_table",
                "client_ip", "*14.175*", "___row_id", "lt", 10000),
            "SELECT COUNT(*) FROM test_table WHERE client_ip LIKE '%14.175%' AND ___row_id < 10000");

        // Q31: Expensive wildcard + 5 disjoint ___row_id ranges.
        // Each range is ~10K rows with large gaps → collector called per range.
        compare(reader, runtime, pb, "Q31",
            pb.buildCollectorWithRangesCount("test_table",
                "client_ip", "*14.175*", "___row_id",
                new int[][]{{0, 10000}, {100000, 110000}, {300000, 310000}, {600000, 610000}, {900000, 910000}}),
            "SELECT COUNT(*) FROM test_table WHERE client_ip LIKE '%14.175%' AND (___row_id BETWEEN 0 AND 9999 OR ___row_id BETWEEN 100000 AND 109999 OR ___row_id BETWEEN 300000 AND 309999 OR ___row_id BETWEEN 600000 AND 609999 OR ___row_id BETWEEN 900000 AND 909999)");

        // Q32: Expensive wildcard + 3 narrow ranges (1K each).
        // Very selective — only ~3K rows survive page pruning per RG.
        compare(reader, runtime, pb, "Q32",
            pb.buildCollectorWithRangesCount("test_table",
                "backend_ip", "*99.99*", "___row_id",
                new int[][]{{5000, 6000}, {500000, 501000}, {1000000, 1001000}}),
            "SELECT COUNT(*) FROM test_table WHERE backend_ip LIKE '%99.99%' AND (___row_id BETWEEN 5000 AND 5999 OR ___row_id BETWEEN 500000 AND 500999 OR ___row_id BETWEEN 1000000 AND 1000999)");

        // Q33: Fat wildcard (matches everything) + 5 ranges.
        // Collector is cheap (all docs match), but page pruning still narrows.
        compare(reader, runtime, pb, "Q33",
            pb.buildCollectorWithRangesCount("test_table",
                "http_version", "HTTP/*", "___row_id",
                new int[][]{{0, 10000}, {200000, 210000}, {400000, 410000}, {700000, 710000}, {1000000, 1010000}}),
            "SELECT COUNT(*) FROM test_table WHERE http_version LIKE 'HTTP/%' AND (___row_id BETWEEN 0 AND 9999 OR ___row_id BETWEEN 200000 AND 209999 OR ___row_id BETWEEN 400000 AND 409999 OR ___row_id BETWEEN 700000 AND 709999 OR ___row_id BETWEEN 1000000 AND 1009999)");

        // ── Expensive *1* wildcard on client_ip (95% match, 77K terms) ──

        // Q34: *1* wildcard + row_id < 10K. 99.6% pages pruned.
        // Lucene enumerates 77K terms — very expensive FFM call.
        compare(reader, runtime, pb, "Q34",
            pb.buildAndCollectorPredicateCount("test_table",
                "client_ip", "*1*", "___row_id", "lt", 10000),
            "SELECT COUNT(*) FROM test_table WHERE client_ip LIKE '%1%' AND ___row_id < 10000");

        // Q35: *1* wildcard + 5 disjoint 10K ranges.
        compare(reader, runtime, pb, "Q35",
            pb.buildCollectorWithRangesCount("test_table",
                "client_ip", "*1*", "___row_id",
                new int[][]{{0, 10000}, {100000, 110000}, {300000, 310000}, {600000, 610000}, {900000, 910000}}),
            "SELECT COUNT(*) FROM test_table WHERE client_ip LIKE '%1%' AND (___row_id BETWEEN 0 AND 9999 OR ___row_id BETWEEN 100000 AND 109999 OR ___row_id BETWEEN 300000 AND 309999 OR ___row_id BETWEEN 600000 AND 609999 OR ___row_id BETWEEN 900000 AND 909999)");

        // Q36: *1* wildcard + 3 narrow 1K ranges. Maximum pruning.
        compare(reader, runtime, pb, "Q36",
            pb.buildCollectorWithRangesCount("test_table",
                "client_ip", "*1*", "___row_id",
                new int[][]{{5000, 6000}, {500000, 501000}, {1000000, 1001000}}),
            "SELECT COUNT(*) FROM test_table WHERE client_ip LIKE '%1%' AND (___row_id BETWEEN 5000 AND 5999 OR ___row_id BETWEEN 500000 AND 500999 OR ___row_id BETWEEN 1000000 AND 1000999)");

        // Q37: *1* wildcard only (no range filter) — baseline for FFM cost.
        compare(reader, runtime, pb, "Q37",
            pb.buildCollectorOnlyCount("test_table", "client_ip", "*1*"),
            "SELECT COUNT(*) FROM test_table WHERE client_ip LIKE '%1%'");

        // Q38: *1* wildcard + 70 disjoint 1K ranges. Tests whether
        // many small FFM calls degrade vs one full-range call.
        compare(reader, runtime, pb, "Q38",
            pb.buildCollectorWithRangesCount("test_table",
                "client_ip", "*1*", "___row_id",
                new int[][]{{0, 1000}, {15000, 16000}, {30000, 31000}, {45000, 46000}, {60000, 61000}, {75000, 76000}, {90000, 91000}, {105000, 106000}, {120000, 121000}, {135000, 136000}, {150000, 151000}, {165000, 166000}, {180000, 181000}, {195000, 196000}, {210000, 211000}, {225000, 226000}, {240000, 241000}, {255000, 256000}, {270000, 271000}, {285000, 286000}, {300000, 301000}, {315000, 316000}, {330000, 331000}, {345000, 346000}, {360000, 361000}, {375000, 376000}, {390000, 391000}, {405000, 406000}, {420000, 421000}, {435000, 436000}, {450000, 451000}, {465000, 466000}, {480000, 481000}, {495000, 496000}, {510000, 511000}, {525000, 526000}, {540000, 541000}, {555000, 556000}, {570000, 571000}, {585000, 586000}, {600000, 601000}, {615000, 616000}, {630000, 631000}, {645000, 646000}, {660000, 661000}, {675000, 676000}, {690000, 691000}, {705000, 706000}, {720000, 721000}, {735000, 736000}, {750000, 751000}, {765000, 766000}, {780000, 781000}, {795000, 796000}, {810000, 811000}, {825000, 826000}, {840000, 841000}, {855000, 856000}, {870000, 871000}, {885000, 886000}, {900000, 901000}, {915000, 916000}, {930000, 931000}, {945000, 946000}, {960000, 961000}, {975000, 976000}, {990000, 991000}, {1005000, 1006000}, {1020000, 1021000}, {1035000, 1036000}}),
            "SELECT COUNT(*) FROM test_table WHERE client_ip LIKE '%1%' AND ___row_id < 1036000");

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
