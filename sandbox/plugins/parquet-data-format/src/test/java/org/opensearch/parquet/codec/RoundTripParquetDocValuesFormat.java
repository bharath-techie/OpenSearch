/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Test-only round-trip {@link DocValuesFormat} that funnels Lucene's exhaustive
 * {@code BaseDocValuesFormatTestCase} battery through the REAL Parquet read stack.
 *
 * <p>Write side: buffers NUMERIC / BINARY / SORTED_NUMERIC doc values handed over by
 * {@code IndexWriter} (including merges) and spills them to a genuine Parquet file via
 * {@link NativeParquetWriter}, stamping the file's path into the segment attributes exactly like
 * the composite engine does. SORTED / SORTED_SET fields delegate to {@link Lucene90DocValuesFormat}:
 * without the composite engine's sidecar terms index, the Parquet sorted path is deliberately
 * non-contractual (streaming fail-fast), so running the sorted battery against it would only
 * measure known refusals rather than find bugs.
 *
 * <p>Read side: {@link ParquetDocValuesProducer} — the production producer, DataFusion decode
 * path, page cache, skipper and all.
 */
public final class RoundTripParquetDocValuesFormat extends DocValuesFormat {

    public static final String NAME = "RoundTripParquet";

    /** Spill directory for the current test run; set by the test before indexing. */
    public static volatile Path SPILL_DIR;

    /**
     * Dictionary-tier cardinality budget for the current test run (mirrors
     * {@code parquet.docvalues.dictionary.max_terms}). Randomized by the test so the battery
     * exercises BOTH ordinal tiers: tiny values force the disk-backed uninverted tier, large
     * values keep fields on the heap dictionary tier.
     */
    public static volatile int DICTIONARY_MAX_TERMS = 65536;

    private final Lucene90DocValuesFormat sortedDelegate = new Lucene90DocValuesFormat();
    private final Path spillDir;

    /** No-arg constructor for Lucene SPI (read path resolves the format by name). */
    public RoundTripParquetDocValuesFormat() {
        super(NAME);
        this.spillDir = null;
    }

    private Path spillDir() {
        Path dir = spillDir != null ? spillDir : SPILL_DIR;
        if (dir == null) {
            throw new IllegalStateException("RoundTripParquetDocValuesFormat.SPILL_DIR not set by the test");
        }
        return dir;
    }

    public RoundTripParquetDocValuesFormat(Path spillDir) {
        super(NAME);
        this.spillDir = spillDir;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new RecordingConsumer(state, sortedDelegate.fieldsConsumer(state), spillDir());
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        DocValuesProducer sorted = null;
        boolean hasSorted = false;
        for (FieldInfo fi : state.fieldInfos) {
            DocValuesType t = fi.getDocValuesType();
            if (t == DocValuesType.SORTED || t == DocValuesType.SORTED_SET) {
                hasSorted = true;
            }
        }
        if (hasSorted) {
            // The delegate consumer always ran (its files exist even with zero entries), so
            // opening it is safe; routing decides per field.
            sorted = sortedDelegate.fieldsProducer(state);
        }
        // The consumer stamps the attribute whenever it spilled ANY field — numeric shapes or
        // sidecar-backed sorted fields — so attribute presence alone decides.
        ParquetDocValuesProducer parquet = null;
        Path parquetFile = null;
        String attr = state.segmentInfo.getAttribute(ParquetSegmentLayout.PARQUET_FILE_ATTRIBUTE);
        if (attr != null) {
            parquet = new ParquetDocValuesProducer(state, null);
            parquetFile = Path.of(attr);
        }
        return new RoutingProducer(parquet, sorted, parquetFile, state, spillDir());
    }

    static Path sidecarPath(Path parquetFile, String field) {
        return parquetFile.resolveSibling(parquetFile.getFileName() + "." + field + ".terms");
    }

    /**
     * Routes reads: numeric shapes to the parquet stack; sorted shapes with a terms sidecar to
     * the REAL ordinal tiers ({@link TermDictionary} within budget, disk-backed
     * {@link UninvertedOrdinals} above it — coverage verification included); sorted shapes
     * without a sidecar (multi-valued sortedset, skipper fields) to the Lucene90 delegate.
     */
    private static final class RoutingProducer extends DocValuesProducer {
        private final ParquetDocValuesProducer parquet;
        private final DocValuesProducer sorted;
        private final Path parquetFile;
        private final SegmentReadState state;
        private final Path ordsDir;
        private final List<UninvertedOrdinals> openedOrdinals = new ArrayList<>();

        RoutingProducer(ParquetDocValuesProducer parquet, DocValuesProducer sorted, Path parquetFile, SegmentReadState state, Path ordsDir) {
            this.parquet = parquet;
            this.sorted = sorted;
            this.parquetFile = parquetFile;
            this.state = state;
            this.ordsDir = ordsDir;
        }

        /** The production tier ladder, minus the node-level caches (fresh per producer). */
        private SortedDocValues tieredSorted(FieldInfo field) throws IOException {
            Path sidecar = sidecarPath(parquetFile, field.name);
            org.apache.lucene.index.Terms terms = SidecarTerms.read(sidecar);
            org.opensearch.parquet.codec.iter.ParquetSortedDocValues streaming =
                (org.opensearch.parquet.codec.iter.ParquetSortedDocValues) parquet.getSorted(field);
            TermDictionary dictionary = TermDictionary.load(terms, DICTIONARY_MAX_TERMS);
            if (dictionary != null) {
                return new org.opensearch.parquet.codec.iter.ParquetDictionarySortedDocValues(streaming, dictionary);
            }
            long expectedNonNull = parquet.nonNullRowCount(field);
            UninvertedOrdinals ordinals = UninvertedOrdinals.build(
                ordsDir.resolve("ords"),
                org.apache.lucene.util.StringHelper.idToString(state.segmentInfo.getId()) + "-" + field.name,
                terms,
                state.segmentInfo.maxDoc(),
                expectedNonNull,
                () -> false
            );
            openedOrdinals.add(ordinals);
            return new org.opensearch.parquet.codec.iter.ParquetUninvertedSortedDocValues(
                ordinals,
                streaming,
                state.segmentInfo.maxDoc()
            );
        }

        private boolean hasSidecar(FieldInfo field) {
            return parquetFile != null && java.nio.file.Files.exists(sidecarPath(parquetFile, field.name));
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) throws IOException {
            return parquet.getNumeric(field);
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo field) throws IOException {
            return parquet.getBinary(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            return parquet.getSortedNumeric(field);
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) throws IOException {
            return hasSidecar(field) ? tieredSorted(field) : sorted.getSorted(field);
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
            // Single-valued fields round-trip through our tiers with the production singleton
            // convention; multi-valued fields (no sidecar) stay on the delegate.
            return hasSidecar(field)
                ? org.apache.lucene.index.DocValues.singleton(tieredSorted(field))
                : sorted.getSortedSet(field);
        }

        @Override
        public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
            DocValuesType t = field.getDocValuesType();
            if (t == DocValuesType.SORTED || t == DocValuesType.SORTED_SET) {
                return sorted.getSkipper(field);
            }
            return parquet.getSkipper(field);
        }

        @Override
        public void checkIntegrity() throws IOException {
            if (parquet != null) {
                parquet.checkIntegrity();
            }
            if (sorted != null) {
                sorted.checkIntegrity();
            }
        }

        @Override
        public void close() throws IOException {
            IOException first = null;
            for (UninvertedOrdinals ordinals : openedOrdinals) {
                try {
                    ordinals.close();
                } catch (IOException e) {
                    if (first == null) {
                        first = e;
                    }
                }
            }
            if (parquet != null) {
                try {
                    parquet.close();
                } catch (IOException e) {
                    if (first == null) {
                        first = e;
                    }
                }
            }
            if (sorted != null) {
                sorted.close();
            }
            if (first != null) {
                throw first;
            }
        }
    }

    /** Buffers parquet-bound fields; delegates sorted shapes; spills parquet on close. */
    private static final class RecordingConsumer extends DocValuesConsumer {

        private final SegmentWriteState state;
        private final DocValuesConsumer sortedDelegate;
        private final Path spillDir;
        private final int maxDoc;

        // field name → per-doc values; null element = missing document.
        private final Map<String, long[][]> numericFields = new LinkedHashMap<>();
        private final Map<String, byte[][]> binaryFields = new LinkedHashMap<>();
        private final Map<String, long[][]> sortedNumericFields = new LinkedHashMap<>();
        // Sorted fields served by OUR tiers: flat binary column + terms sidecar.
        private final Map<String, byte[][]> sortedValueFields = new LinkedHashMap<>();
        private final Map<String, List<BytesRef>> sortedTerms = new LinkedHashMap<>();
        private final Map<String, List<int[]>> sortedPostings = new LinkedHashMap<>();

        RecordingConsumer(SegmentWriteState state, DocValuesConsumer sortedDelegate, Path spillDir) {
            this.state = state;
            this.sortedDelegate = sortedDelegate;
            this.spillDir = spillDir;
            this.maxDoc = state.segmentInfo.maxDoc();
        }

        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            NumericDocValues values = valuesProducer.getNumeric(field);
            long[][] perDoc = new long[maxDoc][];
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                perDoc[doc] = new long[] { values.longValue() };
            }
            numericFields.put(field.name, perDoc);
        }

        @Override
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            BinaryDocValues values = valuesProducer.getBinary(field);
            byte[][] perDoc = new byte[maxDoc][];
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                BytesRef v = values.binaryValue();
                perDoc[doc] = new byte[v.length];
                System.arraycopy(v.bytes, v.offset, perDoc[doc], 0, v.length);
            }
            binaryFields.put(field.name, perDoc);
        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
            long[][] perDoc = new long[maxDoc][];
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                long[] docValues = new long[values.docValueCount()];
                for (int i = 0; i < docValues.length; i++) {
                    docValues[i] = values.nextValue();
                }
                perDoc[doc] = docValues;
            }
            sortedNumericFields.put(field.name, perDoc);
        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            if (field.docValuesSkipIndexType() != org.apache.lucene.index.DocValuesSkipIndexType.NONE) {
                // Production declares skip indexes only for numeric shapes; sorted-with-skipper
                // is not our feature, keep it on the delegate for a compliant skipper.
                sortedDelegate.addSortedField(field, valuesProducer);
                return;
            }
            captureSorted(field, valuesProducer.getSorted(field));
        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            if (field.docValuesSkipIndexType() != org.apache.lucene.index.DocValuesSkipIndexType.NONE) {
                sortedDelegate.addSortedSetField(field, valuesProducer);
                return;
            }
            // Our production convention supports single-valued keyword fields (singleton
            // sortedset). Multi-valued sortedset ordinals are a documented unsupported feature:
            // those fields stay on the delegate.
            SortedSetDocValues probe = valuesProducer.getSortedSet(field);
            boolean singleValued = true;
            for (int doc = probe.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = probe.nextDoc()) {
                if (probe.docValueCount() > 1) {
                    singleValued = false;
                    break;
                }
            }
            if (singleValued == false) {
                sortedDelegate.addSortedSetField(field, valuesProducer);
                return;
            }
            SortedSetDocValues values = valuesProducer.getSortedSet(field);
            captureSortedOrds(field, values.getValueCount(), values::lookupOrd, new OrdIterator() {
                @Override
                public int nextDoc() throws IOException {
                    return values.nextDoc();
                }

                @Override
                public int ord() throws IOException {
                    return (int) values.nextOrd();
                }
            });
        }

        private void captureSorted(FieldInfo field, SortedDocValues values) throws IOException {
            captureSortedOrds(field, values.getValueCount(), values::lookupOrd, new OrdIterator() {
                @Override
                public int nextDoc() throws IOException {
                    return values.nextDoc();
                }

                @Override
                public int ord() throws IOException {
                    return values.ordValue();
                }
            });
        }

        private interface OrdIterator {
            int nextDoc() throws IOException;

            int ord() throws IOException;
        }

        private void captureSortedOrds(FieldInfo field, long valueCount, OrdToTerm lookupOrd, OrdIterator it) throws IOException {
            List<BytesRef> terms = new ArrayList<>((int) valueCount);
            for (int ord = 0; ord < valueCount; ord++) {
                terms.add(BytesRef.deepCopyOf(lookupOrd.term(ord)));
            }
            byte[][] perDoc = new byte[maxDoc][];
            List<List<Integer>> postings = new ArrayList<>((int) valueCount);
            for (int ord = 0; ord < valueCount; ord++) {
                postings.add(new ArrayList<>());
            }
            for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                int ord = it.ord();
                BytesRef term = terms.get(ord);
                perDoc[doc] = new byte[term.length];
                System.arraycopy(term.bytes, term.offset, perDoc[doc], 0, term.length);
                postings.get(ord).add(doc);
            }
            List<int[]> postingArrays = new ArrayList<>((int) valueCount);
            for (List<Integer> docs : postings) {
                postingArrays.add(docs.stream().mapToInt(Integer::intValue).toArray());
            }
            sortedValueFields.put(field.name, perDoc);
            sortedTerms.put(field.name, terms);
            sortedPostings.put(field.name, postingArrays);
        }

        private interface OrdToTerm {
            BytesRef term(int ord) throws IOException;
        }

        @Override
        public void close() throws IOException {
            try {
                boolean any = numericFields.isEmpty() == false
                    || binaryFields.isEmpty() == false
                    || sortedNumericFields.isEmpty() == false
                    || sortedValueFields.isEmpty() == false;
                if (any) {
                    Path file = spillDir.resolve(state.segmentInfo.name + "_" + UUID.randomUUID() + ".parquet");
                    writeParquet(file);
                    for (String field : sortedTerms.keySet()) {
                        SidecarTerms.write(sidecarPath(file, field), sortedTerms.get(field), sortedPostings.get(field));
                    }
                    state.segmentInfo.putAttribute(ParquetSegmentLayout.PARQUET_FILE_ATTRIBUTE, file.toString());
                }
            } finally {
                sortedDelegate.close();
            }
        }

        private void writeParquet(Path file) throws IOException {
            List<Field> arrowFields = new ArrayList<>();
            for (String name : numericFields.keySet()) {
                arrowFields.add(new Field(name, FieldType.nullable(new ArrowType.Int(64, true)), null));
            }
            for (String name : binaryFields.keySet()) {
                arrowFields.add(new Field(name, FieldType.nullable(new ArrowType.Binary()), null));
            }
            for (String name : sortedValueFields.keySet()) {
                arrowFields.add(new Field(name, FieldType.nullable(new ArrowType.Binary()), null));
            }
            for (String name : sortedNumericFields.keySet()) {
                Field item = new Field("item", FieldType.nullable(new ArrowType.Int(64, true)), null);
                arrowFields.add(new Field(name, FieldType.nullable(new ArrowType.List()), List.of(item)));
            }
            Schema schema = new Schema(arrowFields);

            try (BufferAllocator allocator = new RootAllocator()) {
                NativeParquetWriter writer = new NativeParquetWriter(file.toString());
                ArrowSchema schemaExport = ArrowSchema.allocateNew(allocator);
                Data.exportSchema(allocator, schema, null, schemaExport);
                try (ArrowExport export = new ArrowExport(null, schemaExport)) {
                    writer.initialize("dv-roundtrip-test", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
                }
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                    for (Map.Entry<String, long[][]> e : numericFields.entrySet()) {
                        BigIntVector vec = (BigIntVector) root.getVector(e.getKey());
                        for (int doc = 0; doc < maxDoc; doc++) {
                            long[] v = e.getValue()[doc];
                            if (v == null) {
                                vec.setNull(doc);
                            } else {
                                vec.setSafe(doc, v[0]);
                            }
                        }
                    }
                    Map<String, byte[][]> allBinary = new LinkedHashMap<>(binaryFields);
                    allBinary.putAll(sortedValueFields);
                    for (Map.Entry<String, byte[][]> e : allBinary.entrySet()) {
                        VarBinaryVector vec = (VarBinaryVector) root.getVector(e.getKey());
                        for (int doc = 0; doc < maxDoc; doc++) {
                            byte[] v = e.getValue()[doc];
                            if (v == null) {
                                vec.setNull(doc);
                            } else {
                                vec.setSafe(doc, v);
                            }
                        }
                    }
                    for (Map.Entry<String, long[][]> e : sortedNumericFields.entrySet()) {
                        ListVector vec = (ListVector) root.getVector(e.getKey());
                        UnionListWriter listWriter = vec.getWriter();
                        for (int doc = 0; doc < maxDoc; doc++) {
                            long[] v = e.getValue()[doc];
                            listWriter.setPosition(doc);
                            if (v != null) {
                                listWriter.startList();
                                for (long value : v) {
                                    listWriter.writeBigInt(value);
                                }
                                listWriter.endList();
                            }
                        }
                        vec.setValueCount(maxDoc);
                    }
                    root.setRowCount(maxDoc);

                    ArrowArray array = ArrowArray.allocateNew(allocator);
                    ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
                    Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
                    try (ArrowExport export = new ArrowExport(array, arrowSchema)) {
                        writer.write(export.getArrayAddress(), export.getSchemaAddress());
                    }
                }
                writer.flush();
            }
        }
    }
}
