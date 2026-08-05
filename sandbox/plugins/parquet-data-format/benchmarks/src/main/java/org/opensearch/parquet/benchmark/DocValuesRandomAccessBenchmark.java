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
import org.opensearch.parquet.bridge.DataFusionColumnReader;
import org.opensearch.parquet.bridge.NativeParquetWriter;
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
 * JMH benchmark for {@link ParquetNumericDocValues#advanceExact} over the DataFusion decode
 * path ({@link DataFusionColumnReader}: retained Arrow cursor, AIMD batch window, page-level
 * OffsetIndex skips, PageCache-resident batches).
 *
 * <p>Successor to the retired benchmark of the same name that measured the deleted
 * codec-native {@code ParquetColumnReader}; access patterns are kept identical so historical
 * numbers remain roughly comparable.
 *
 * <p>Access patterns:
 * <ul>
 *   <li>{@code sequentialScan} — ascending doc IDs: the cursor's home turf; the AIMD window
 *       grows and almost every call is a resident-batch hit.</li>
 *   <li>{@code randomAccess} — uniform random doc IDs across the whole file: forward targets
 *       ride cheap skips, backward targets exercise {@code parquet_df_reset_iter}'s cheap
 *       cursor rewind (the fix that killed the reopen storm).</li>
 *   <li>{@code pageMissPingPong} — alternates between the first and last page: every call is
 *       a worst-case long-distance reposition.</li>
 *   <li>{@code openReadClose} — per-query lifecycle: open a fresh cursor, touch a few
 *       scattered docs, close. Measures the metadata/open overhead a real search pays once
 *       per (field, producer).</li>
 * </ul>
 *
 * <p>{@code nullFraction} covers the required-column and nullable presence paths;
 * {@code columnType} covers the i64 passthrough (INT64) and widening (INT32) conversions.
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

    /** Fraction of null rows. 0.0 → required-column fast paths; 0.3 → nullable presence paths. */
    @Param({ "0.0", "0.3" })
    private double nullFraction;

    /** INT64 = raw i64 passthrough; INT32 = widening conversion (vectorizable). */
    @Param({ "INT64", "INT32" })
    private String columnType;

    /** Number of advanceExact calls per invocation (fixed target array, fixed seed). */
    private static final int TARGETS = 100_000;
    /** Rows per Arrow batch handed to the native writer during setup. */
    private static final int BATCH_ROWS = 100_000;
    /** Starting AIMD decode window, matching the production default. */
    private static final int INITIAL_BATCH_SIZE = 32;

    private BufferAllocator allocator;
    private Path file;
    private BufferPool bufferPool;
    private DataFusionColumnReader reader;
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
        reader = DataFusionColumnReader.open(file, column(), physical(), false, bufferPool, INITIAL_BATCH_SIZE);
        docValues = new ParquetNumericDocValues(reader, rows);

        // Fixed seed so every fork/param combination replays the identical target sequence.
        Random random = new Random(42);
        randomTargets = new int[TARGETS];
        for (int i = 0; i < TARGETS; i++) {
            randomTargets[i] = random.nextInt(rows);
        }
        // Alternate between the first and last page so every call is a long reposition.
        pingPongTargets = new int[TARGETS];
        for (int i = 0; i < TARGETS; i++) {
            pingPongTargets[i] = (i % 2 == 0) ? (i / 2) % 1000 : rows - 1 - ((i / 2) % 1000);
        }
    }

    private String column() {
        return columnType.equals("INT64") ? "val_i64" : "val_i32";
    }

    private ParquetPhysicalType physical() {
        return columnType.equals("INT64") ? ParquetPhysicalType.INT64 : ParquetPhysicalType.INT32;
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
     * Baseline: ascending scan of the whole column. The AIMD window converges to its ceiling
     * and per-call cost is a resident-batch array read.
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
     * Uniform random targets: mostly resident misses. Forward jumps use page skips; backward
     * jumps use the cheap cursor rewind instead of a file reopen.
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

    /** Worst case: every advanceExact is a full-length reposition (first page ⇄ last page). */
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
     * Per-query lifecycle cost: open a fresh cursor (the once-per-field-per-query step a real
     * search pays for every producer), read a handful of scattered docs, close. Dominated by
     * {@code parquet_df_open_iter}'s metadata work, which the node-level metadata caches
     * convert from a per-open parse into a lookup.
     */
    @Benchmark
    public long openReadClose(Blackhole bh) throws IOException {
        try (
            BufferPool pool = new BufferPool();
            DataFusionColumnReader r = DataFusionColumnReader.open(file, column(), physical(), false, pool, INITIAL_BATCH_SIZE)
        ) {
            ParquetNumericDocValues dv = new ParquetNumericDocValues(r, rows);
            long sum = 0;
            // Touch a few scattered docs so the open isn't dead-code-eliminated and the reader
            // exercises a realistic first-access pattern.
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
