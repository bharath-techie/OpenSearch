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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;
import org.opensearch.be.datafusion.indexfilter.CollectorRegistry;
import org.opensearch.be.datafusion.indexfilter.FilterProviderRegistry;
import org.opensearch.be.datafusion.indexfilter.FilterTreeCallbacks;
import org.opensearch.be.datafusion.indexfilter.LuceneTermQueryFactory;
import org.opensearch.be.datafusion.indexfilter.SubstraitPlanBuilder;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.arrow.c.Data.importField;

import io.substrait.proto.Type;

/**
 * E2E perf test — standalone JUnit (no OpenSearchTestCase, no
 * RandomizedRunner) so Rust-spawned threads don't hit context errors.
 */
@LuceneTestCase.SuppressCodecs("*")
public class IndexedQueryPerfTests extends OpenSearchTestCase {

    private static final String DATA_DIR = System.getProperty(
        "perf.data.dir",
        "/Users/abandeji/Public/work-dump/experiments/data/nodes/0/indices/ATTb8ViJT0mrLoeWpkEtrA/0"
    );

    public void testPhase1_AndCollectorPredicate() throws Exception {
        // Disable randomized testing assertions so Rust-spawned threads
        // can call Lucene APIs without hitting RandomizedContext checks.
        System.setProperty("tests.asserts.gracious", "true");

        Path shardDir = Path.of(DATA_DIR);
        Path luceneDir = shardDir.resolve("index");
        Path parquetDir = shardDir.resolve("parquet");

        // 1. Init native runtime
        NativeBridge.initTokioRuntimeManager(4);
        Path spillDir = createTempDir("perf-spill");
        NativeRuntimeHandle runtime = new NativeRuntimeHandle(
            NativeBridge.createGlobalRuntime(512 * 1024 * 1024, 0L, spillDir.toString(), 128 * 1024 * 1024)
        );

        // 2. Register Lucene factory
        CollectorRegistry collectors = new CollectorRegistry();
        FilterProviderRegistry providers = new FilterProviderRegistry(collectors);
        LuceneTermQueryFactory factory = new LuceneTermQueryFactory(luceneDir);
        providers.setFactory(factory);
        FilterTreeCallbacks.setRegistries(providers, collectors);

        // 3. Create reader
        String[] parquetFiles = { "generation-1.parquet", "generation-2.parquet" };
        ReaderHandle reader = new ReaderHandle(parquetDir.toString(), parquetFiles);

        // 4. Build substrait plan — schema matches parquet files
        List<String> columns = List.of(
            "backend_ip", "backend_port", "backend_processing_time", "backend_status_code",
            "client_ip", "client_port", "connection_time", "destination_ip", "destination_port",
            "elb_status_code", "http_port", "http_version", "matched_rule_priority",
            "received_bytes", "request_creation_time", "request_processing_time",
            "response_processing_time", "sent_bytes", "target_ip", "target_port",
            "target_processing_time", "target_status_code", "timestamp", "___row_id", "_id"
        );
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
            SubstraitPlanBuilder.binaryType()
        );

        SubstraitPlanBuilder planBuilder = new SubstraitPlanBuilder(columns, types);
        byte[] substraitBytes = planBuilder.buildAndCollectorPredicate(
            "test_table", "client_ip", "14.175.54.83", "elb_status_code", "eq", 200
        );

        // 5. Execute and measure
        System.out.println("=== Phase 1: AND(TermQuery(client_ip=14.175.54.83), elb_status_code=200) ===");

        long warmupRows = executeAndCount(reader, runtime, substraitBytes);
        System.out.println("Warmup: " + warmupRows + " rows");

        int runs = 5;
        long totalRows = 0;
        long totalNanos = 0;
        for (int i = 0; i < runs; i++) {
            long start = System.nanoTime();
            long rows = executeAndCount(reader, runtime, substraitBytes);
            long elapsed = System.nanoTime() - start;
            totalRows += rows;
            totalNanos += elapsed;
            System.out.printf("  run %d: %d rows in %.1f ms%n", i + 1, rows, elapsed / 1e6);
        }
        System.out.printf("Average: %d rows in %.1f ms (%.0f rows/sec)%n",
            totalRows / runs, totalNanos / runs / 1e6, totalRows / (totalNanos / 1e9));

        // Cleanup
        reader.close();
        factory.closeReader();
        FilterTreeCallbacks.setRegistries(null, null);
        NativeBridge.closeGlobalRuntime(runtime.getPointer());
    }

    private long executeAndCount(ReaderHandle reader, NativeRuntimeHandle runtime, byte[] plan) throws Exception {
        long streamPtr = asyncCall(listener ->
            NativeBridge.executeQueryAsync(reader.getPointer(), "test_table", plan, runtime.getPointer(), 0L, listener)
        );
        assertTrue("stream pointer should be non-zero", streamPtr != 0);

        long totalRows = 0;
        try (
            StreamHandle stream = new StreamHandle(streamPtr, runtime);
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()
        ) {
            long schemaAddr = asyncCall(listener -> NativeBridge.streamGetSchema(stream.getPointer(), listener));
            Schema schema = new Schema(importField(allocator, ArrowSchema.wrap(schemaAddr), dictProvider).getChildren(), null);
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

            while (true) {
                long arrayAddr = asyncCall(listener -> NativeBridge.streamNext(runtime.getPointer(), stream.getPointer(), listener));
                if (arrayAddr == 0) break;
                Data.importIntoVectorSchemaRoot(allocator, ArrowArray.wrap(arrayAddr), root, dictProvider);
                totalRows += root.getRowCount();
            }
            root.close();
        }
        return totalRows;
    }

    private long asyncCall(java.util.function.Consumer<ActionListener<Long>> call) throws Exception {
        CompletableFuture<Long> future = new CompletableFuture<>();
        call.accept(new ActionListener<>() {
            @Override public void onResponse(Long v) { future.complete(v); }
            @Override public void onFailure(Exception e) { future.completeExceptionally(e); }
        });
        return future.get();
    }
}
