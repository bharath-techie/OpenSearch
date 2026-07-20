/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.benchmark;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetColumnReader;
import org.opensearch.parquet.bridge.ParquetSortConfig;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.codec.ParquetPhysicalType;
import org.opensearch.parquet.codec.cache.BufferPool;
import org.opensearch.parquet.codec.iter.ParquetNumericDocValues;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for {@link ParquetNumericDocValues#advanceExact} under different access
 * patterns, isolating the native page-decode path (`parquet_decode_page_at_row`).
 *
 * <p>Access patterns:
 * <ul>
 *   <li>{@code sequentialScan} — ascending doc IDs: almost all L1/L2 PageCache hits, one FFM
 *       decode per page (~rows/20k decodes total). Baseline; largely unaffected by decode-path
 *       changes.</li>
 *   <li>{@code randomAccess} — uniform random doc IDs across the whole file: with ~50 pages
 *       resident-page hit probability is ~2%, so nearly every call is a cold page decode.
 *       This is the pattern the zero-alloc/branchless decode work targets.</li>
 *   <li>{@code pageMissPingPong} — alternates between the first and last page: every call
 *       evicts the resident page, giving a pure worst-case decode measurement.</li>
 * </ul>
 *
 * <p>Parameters cover both presence-pack paths (required-column memset vs branchless
 * def-level compare) and both value-expand paths (all-present tight widening loop vs
 * bitset pop-and-scatter): {@code nullFraction=0.0} exercises the former of each pair,
 * {@code nullFraction=0.3} the latter. {@code columnType} covers the i64 passthrough
 * (INT64) and the SIMD-widening (INT32) conversions.
 *
 * <p>Run with:
 * <pre>
 * ./gradlew -Dsandbox.enabled=true :sandbox:plugins:parquet-data-format:benchmarks:run \
 *   --args 'DocValuesRandomAccessBenchmark'
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class DocValuesRandomAccessBenchmark {

    /** Total rows in the benchmark file. 1M rows / default 20k page_row_limit ≈ 50 pages. */
    @Param({ "1000000" })
    private int rows;

    /** Fraction of null rows. 0.0 → required-column fast paths; 0.3 → nullable scatter paths. */
    @Param({ "0.0", "0.3" })
    private double nullFraction;

    /** INT64 = raw i64 passthrough; INT32 = widening conversion (vectorizable). */
    @Param({ "INT64", "INT32" })
    private String columnType;

    /** Number of advanceExact calls per invocation (fixed target array, fixed seed). */
    private static final int TARGETS = 100_000;
    /** Rows per Arrow batch handed to the native writer during setup. */
    private static final int BATCH_ROWS = 100_000;

    private BufferAllocator allocator;
    private Path file;
    private BufferPool bufferPool;
    private ParquetColumnReader reader;
    private ParquetNumericDocValues docValues;
    private int[] randomTargets;
    private int[] pingPongTargets;

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        RustBridge.initLogger();
        allocator = new RootAllocator();
        file = Files.createTempDirectory("dv-bench").resolve("bench.parquet");
        writeFile();

        bufferPool = new BufferPool();
        String column = columnType.equals("INT64") ? "val_i64" : "val_i32";
        ParquetPhysicalType physical = columnType.equals("INT64") ? ParquetPhysicalType.INT64 : ParquetPhysicalType.INT32;
        reader = ParquetColumnReader.open(file, column, physical, false, bufferPool);
        docValues = new ParquetNumericDocValues(reader, rows);

        // Fixed seed so every fork/param combination replays the identical target sequence.
        Random random = new Random(42);
        randomTargets = new int[TARGETS];
        for (int i = 0; i < TARGETS; i++) {
            randomTargets[i] = random.nextInt(rows);
        }
        // Alternate between the first and last page so every call is a page miss.
        pingPongTargets = new int[TARGETS];
        for (int i = 0; i < TARGETS; i++) {
            pingPongTargets[i] = (i % 2 == 0) ? (i / 2) % 1000 : rows - 1 - ((i / 2) % 1000);
        }
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws Exception {
        if (reader != null) {
            reader.close();
        }
        if (bufferPool != null) {
            bufferPool.close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    /**
     * Baseline: ascending scan of the whole column. L1/L2 hit rate ≈ (1 - pages/rows);
     * decode cost is amortized over ~20k rows per FFM call.
     */
    @Benchmark
    @OperationsPerInvocation(1_000_000)
    public void sequentialScan(Blackhole bh) throws IOException {
        for (int d = 0; d < rows; d++) {
            if (docValues.advanceExact(d)) {
                bh.consume(docValues.longValue());
            }
        }
    }

    /**
     * The optimized case: uniform random targets, ~98% resident-page misses, so throughput
     * is dominated by the native page decode (scratch reuse + branchless presence pack +
     * direct-to-outbuf expand).
     */
    @Benchmark
    @OperationsPerInvocation(TARGETS)
    public void randomAccess(Blackhole bh) throws IOException {
        for (int t : randomTargets) {
            if (docValues.advanceExact(t)) {
                bh.consume(docValues.longValue());
            }
        }
    }

    /** Worst case: every advanceExact evicts the resident page — a pure cold-decode measurement. */
    @Benchmark
    @OperationsPerInvocation(TARGETS)
    public void pageMissPingPong(Blackhole bh) throws IOException {
        for (int t : pingPongTargets) {
            if (docValues.advanceExact(t)) {
                bh.consume(docValues.longValue());
            }
        }
    }

    /**
     * Per-query lifecycle cost: open a fresh column reader (the once-per-field-per-query step a
     * real search pays for every producer), read a handful of scattered docs, close. Dominated
     * by {@link ParquetColumnReader#open}'s metadata work — schema resolution + page-layout
     * (OffsetIndex/ColumnIndex) computation + ColumnPageIndex marshal — which the node-level
     * file-metadata cache converts from a per-open parse into an Arc-clone lookup. This is the
     * benchmark that shows the "dvm-equivalent" win; the decode benchmarks above open once per
     * trial and cannot see it.
     */
    @Benchmark
    public long openReadClose(Blackhole bh) throws IOException {
        String column = columnType.equals("INT64") ? "val_i64" : "val_i32";
        ParquetPhysicalType physical = columnType.equals("INT64") ? ParquetPhysicalType.INT64 : ParquetPhysicalType.INT32;
        try (BufferPool pool = new BufferPool(); ParquetColumnReader r = ParquetColumnReader.open(file, column, physical, false, pool)) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(r, rows);
            long sum = 0;
            // Touch a few scattered docs so the open isn't dead-code-eliminated and the reader
            // exercises a realistic first-access pattern (a couple of page decodes).
            for (int i = 0; i < 8; i++) {
                int t = randomTargets[i * (TARGETS / 8)];
                if (dv.advanceExact(t)) {
                    sum += dv.longValue();
                }
            }
            bh.consume(sum);
            return sum;
        }
    }

    // ── data generation ──

    private void writeFile() throws Exception {
        Schema schema = new Schema(
            List.of(
                new Field("val_i64", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("val_i32", FieldType.nullable(new ArrowType.Int(32, true)), null)
            )
        );

        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        try (ArrowExport schemaExport = exportSchema(schema)) {
            writer.initialize("dv-bench-index", schemaExport.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }

        // Deterministic values and null placement so all runs measure identical data.
        Random random = new Random(7);
        for (int start = 0; start < rows; start += BATCH_ROWS) {
            int batch = Math.min(BATCH_ROWS, rows - start);
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                BigIntVector i64Vec = (BigIntVector) root.getVector("val_i64");
                IntVector i32Vec = (IntVector) root.getVector("val_i32");
                for (int i = 0; i < batch; i++) {
                    if (nullFraction > 0 && random.nextDouble() < nullFraction) {
                        i64Vec.setNull(i);
                        i32Vec.setNull(i);
                    } else {
                        i64Vec.setSafe(i, random.nextLong());
                        i32Vec.setSafe(i, random.nextInt());
                    }
                }
                root.setRowCount(batch);

                ArrowArray array = ArrowArray.allocateNew(allocator);
                ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
                Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
                try (ArrowExport export = new ArrowExport(array, arrowSchema)) {
                    writer.write(export.getArrayAddress(), export.getSchemaAddress());
                }
            }
        }
        writer.flush();
    }

    private ArrowExport exportSchema(Schema schema) {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, arrowSchema);
        return new ArrowExport(null, arrowSchema);
    }
}
