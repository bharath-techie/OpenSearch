/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.values;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.DirectWriter;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Copy of Lucene90DocValuesConsumer.
 * Minor changes specific to star tree replacing DISI.
 */
@ExperimentalApi
public class StarTree99ValuesConsumer implements Closeable { // Todo extend consumer if needed, we don't need it right now
    IndexOutput data, meta;
    final int maxDoc;
    private byte[] termsDictBuffer;
    static final byte NUMERIC = 0;
    static final byte BINARY = 1;
    static final byte SORTED = 2;
    static final byte SORTED_SET = 3;
    static final byte SORTED_NUMERIC = 4;
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    static final int NUMERIC_BLOCK_SHIFT = 14;
    static final int NUMERIC_BLOCK_SIZE = 1 << NUMERIC_BLOCK_SHIFT;

    static final int TERMS_DICT_BLOCK_LZ4_SHIFT = 6;
    static final int TERMS_DICT_BLOCK_LZ4_SIZE = 1 << TERMS_DICT_BLOCK_LZ4_SHIFT;
    static final int TERMS_DICT_BLOCK_LZ4_MASK = TERMS_DICT_BLOCK_LZ4_SIZE - 1;

    static final int TERMS_DICT_REVERSE_INDEX_SHIFT = 10;
    static final int TERMS_DICT_REVERSE_INDEX_SIZE = 1 << TERMS_DICT_REVERSE_INDEX_SHIFT;
    static final int TERMS_DICT_REVERSE_INDEX_MASK = TERMS_DICT_REVERSE_INDEX_SIZE - 1;

    /** expert: Creates a new writer */
    public StarTree99ValuesConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension)
        throws IOException {
        this.termsDictBuffer = new byte[1 << 14];
        boolean success = false;
        try {
            String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
            data = state.directory.createOutput(dataName, state.context);
            CodecUtil.writeIndexHeader(
                data,
                dataCodec,
                0, // TODO
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
            meta = state.directory.createOutput(metaName, state.context);
            CodecUtil.writeIndexHeader(
                meta,
                metaCodec,
                0, // TODO
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            maxDoc = state.segmentInfo.maxDoc();
            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    public void close() throws IOException {
        boolean success = false;
        try {
            if (meta != null) {
                meta.writeInt(-1); // write EOF marker
                CodecUtil.writeFooter(meta); // write checksum
            }
            if (data != null) {
                CodecUtil.writeFooter(data); // write checksum
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(data, meta);
            } else {
                IOUtils.closeWhileHandlingException(data, meta);
            }
            meta = data = null;
        }
    }

    public void addNumericField(FieldInfo field, StarTreeValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(NUMERIC);

        writeValues(field, new EmptyStarTreeValuesProducer() {
            @Override
            public StarTreeSortedNumericValues getStarTreeSortedNumericValues(FieldInfo field) throws IOException {
                return StarTreeDocValues.singleton(valuesProducer.getStarTreeNumericValues(field));
            }
        }, false);
    }

    private static class MinMaxTracker {
        long min, max, numValues, spaceInBits;

        MinMaxTracker() {
            reset();
            spaceInBits = 0;
        }

        private void reset() {
            min = Long.MAX_VALUE;
            max = Long.MIN_VALUE;
            numValues = 0;
        }

        /** Accumulate a new value. */
        void update(long v) {
            min = Math.min(min, v);
            max = Math.max(max, v);
            ++numValues;
        }

        /** Accumulate state from another tracker. */
        void update(MinMaxTracker other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            numValues += other.numValues;
        }

        /** Update the required space. */
        void finish() {
            if (max > min) {
                spaceInBits += DirectWriter.unsignedBitsRequired(max - min) * numValues;
            }
        }

        /** Update space usage and get ready for accumulating values for the next block. */
        void nextBlock() {
            finish();
            reset();
        }
    }

    private long[] writeValues(FieldInfo field, StarTreeValuesProducer valuesProducer, boolean ords) throws IOException {
        StarTreeSortedNumericValues values = valuesProducer.getStarTreeSortedNumericValues(field);
        final long firstValue;
        if (values.nextEntry() != StarTreeValuesIterator.NO_MORE_ENTRIES) {
            firstValue = values.nextValue();
        } else {
            firstValue = 0L;
        }
        values = valuesProducer.getStarTreeSortedNumericValues(field);
        int numDocsWithValue = 0;
        MinMaxTracker minMax = new MinMaxTracker();
        MinMaxTracker blockMinMax = new MinMaxTracker();
        long gcd = 0;
        Set<Long> uniqueValues = ords ? null : new HashSet<>();
        for (int doc = values.nextEntry(); doc != StarTreeValuesIterator.NO_MORE_ENTRIES; doc = values.nextEntry()) {
            for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                long v = values.nextValue();

                if (gcd != 1) {
                    if (v < Long.MIN_VALUE / 2 || v > Long.MAX_VALUE / 2) {
                        // in that case v - minValue might overflow and make the GCD computation return
                        // wrong results. Since these extreme values are unlikely, we just discard
                        // GCD computation for them
                        gcd = 1;
                    } else {
                        gcd = MathUtil.gcd(gcd, v - firstValue);
                    }
                }

                blockMinMax.update(v);
                if (blockMinMax.numValues == NUMERIC_BLOCK_SIZE) {
                    minMax.update(blockMinMax);
                    blockMinMax.nextBlock();
                }

                if (uniqueValues != null && uniqueValues.add(v) && uniqueValues.size() > 256) {
                    uniqueValues = null;
                }
            }

            numDocsWithValue++;
        }

        minMax.update(blockMinMax);
        minMax.finish();
        blockMinMax.finish();

        if (ords && minMax.numValues > 0) {
            if (minMax.min != 0) {
                throw new IllegalStateException("The min value for ordinals should always be 0, got " + minMax.min);
            }
            if (minMax.max != 0 && gcd != 1) {
                throw new IllegalStateException("GCD compression should never be used on ordinals, found gcd=" + gcd);
            }
        }

        final long numValues = minMax.numValues;
        long min = minMax.min;
        final long max = minMax.max;
        assert blockMinMax.spaceInBits <= minMax.spaceInBits;

        if (numDocsWithValue == 0) { // meta[-2, 0]: No documents with values
            meta.writeLong(-2); // docsWithFieldOffset
            meta.writeLong(0L); // docsWithFieldLength
            meta.writeShort((short) -1); // jumpTableEntryCount
            meta.writeByte((byte) -1); // denseRankPower
        } else if (numDocsWithValue == maxDoc) { // meta[-1, 0]: All documents has values
            meta.writeLong(-1); // docsWithFieldOffset
            meta.writeLong(0L); // docsWithFieldLength
            meta.writeShort((short) -1); // jumpTableEntryCount
            meta.writeByte((byte) -1); // denseRankPower
        } else { // meta[data.offset, data.length]: IndexedDISI structure for documents with values
            long offset = data.getFilePointer();
            meta.writeLong(offset); // docsWithFieldOffset
            values = valuesProducer.getStarTreeSortedNumericValues(field);
            final short jumpTableEntryCount = StarTreeIndexedValuesIterator.writeBitSet(
                values,
                data,
                StarTreeIndexedValuesIterator.DEFAULT_DENSE_RANK_POWER
            );
            meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
            meta.writeShort(jumpTableEntryCount);
            meta.writeByte(StarTreeIndexedValuesIterator.DEFAULT_DENSE_RANK_POWER);
        }

        meta.writeLong(numValues);
        final int numBitsPerValue;
        boolean doBlocks = false;
        Map<Long, Integer> encode = null;
        if (min >= max) { // meta[-1]: All values are 0
            numBitsPerValue = 0;
            meta.writeInt(-1); // tablesize
        } else {
            if (uniqueValues != null
                && uniqueValues.size() > 1
                && DirectWriter.unsignedBitsRequired(uniqueValues.size() - 1) < DirectWriter.unsignedBitsRequired((max - min) / gcd)) {
                numBitsPerValue = DirectWriter.unsignedBitsRequired(uniqueValues.size() - 1);
                final Long[] sortedUniqueValues = uniqueValues.toArray(new Long[0]);
                Arrays.sort(sortedUniqueValues);
                meta.writeInt(sortedUniqueValues.length); // tablesize
                for (Long v : sortedUniqueValues) {
                    meta.writeLong(v); // table[] entry
                }
                encode = new HashMap<>();
                for (int i = 0; i < sortedUniqueValues.length; ++i) {
                    encode.put(sortedUniqueValues[i], i);
                }
                min = 0;
                gcd = 1;
            } else {
                uniqueValues = null;
                // we do blocks if that appears to save 10+% storage
                doBlocks = minMax.spaceInBits > 0 && (double) blockMinMax.spaceInBits / minMax.spaceInBits <= 0.9;
                if (doBlocks) {
                    numBitsPerValue = 0xFF;
                    meta.writeInt(-2 - NUMERIC_BLOCK_SHIFT); // tablesize
                } else {
                    numBitsPerValue = DirectWriter.unsignedBitsRequired((max - min) / gcd);
                    if (gcd == 1 && min > 0 && DirectWriter.unsignedBitsRequired(max) == DirectWriter.unsignedBitsRequired(max - min)) {
                        min = 0;
                    }
                    meta.writeInt(-1); // tablesize
                }
            }
        }

        meta.writeByte((byte) numBitsPerValue);
        meta.writeLong(min);
        meta.writeLong(gcd);
        long startOffset = data.getFilePointer();
        meta.writeLong(startOffset); // valueOffset
        long jumpTableOffset = -1;
        if (doBlocks) {
            jumpTableOffset = writeValuesMultipleBlocks(valuesProducer.getStarTreeSortedNumericValues(field), gcd);
        } else if (numBitsPerValue != 0) {
            writeValuesSingleBlock(valuesProducer.getStarTreeSortedNumericValues(field), numValues, numBitsPerValue, min, gcd, encode);
        }
        meta.writeLong(data.getFilePointer() - startOffset); // valuesLength
        meta.writeLong(jumpTableOffset);
        return new long[] { numDocsWithValue, numValues };
    }

    private void writeValuesSingleBlock(
        StarTreeSortedNumericValues values,
        long numValues,
        int numBitsPerValue,
        long min,
        long gcd,
        Map<Long, Integer> encode
    ) throws IOException {
        DirectWriter writer = DirectWriter.getInstance(data, numValues, numBitsPerValue);
        for (int doc = values.nextEntry(); doc != StarTreeValuesIterator.NO_MORE_ENTRIES; doc = values.nextEntry()) {
            for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                long v = values.nextValue();
                if (encode == null) {
                    writer.add((v - min) / gcd);
                } else {
                    writer.add(encode.get(v));
                }
            }
        }
        writer.finish();
    }

    // Returns the offset to the jump-table for vBPV
    private long writeValuesMultipleBlocks(StarTreeSortedNumericValues values, long gcd) throws IOException {
        long[] offsets = new long[ArrayUtil.oversize(1, Long.BYTES)];
        int offsetsIndex = 0;
        final long[] buffer = new long[NUMERIC_BLOCK_SIZE];
        final ByteBuffersDataOutput encodeBuffer = ByteBuffersDataOutput.newResettableInstance();
        int upTo = 0;
        for (int doc = values.nextEntry(); doc != StarTreeValuesIterator.NO_MORE_ENTRIES; doc = values.nextEntry()) {
            for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                buffer[upTo++] = values.nextValue();
                if (upTo == NUMERIC_BLOCK_SIZE) {
                    offsets = ArrayUtil.grow(offsets, offsetsIndex + 1);
                    offsets[offsetsIndex++] = data.getFilePointer();
                    writeBlock(buffer, NUMERIC_BLOCK_SIZE, gcd, encodeBuffer);
                    upTo = 0;
                }
            }
        }
        if (upTo > 0) {
            offsets = ArrayUtil.grow(offsets, offsetsIndex + 1);
            offsets[offsetsIndex++] = data.getFilePointer();
            writeBlock(buffer, upTo, gcd, encodeBuffer);
        }

        // All blocks has been written. Flush the offset jump-table
        final long offsetsOrigo = data.getFilePointer();
        for (int i = 0; i < offsetsIndex; i++) {
            data.writeLong(offsets[i]);
        }
        data.writeLong(offsetsOrigo);
        return offsetsOrigo;
    }

    private void writeBlock(long[] values, int length, long gcd, ByteBuffersDataOutput buffer) throws IOException {
        assert length > 0;
        long min = values[0];
        long max = values[0];
        for (int i = 1; i < length; ++i) {
            final long v = values[i];
            assert Math.floorMod(values[i] - min, gcd) == 0;
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        if (min == max) {
            data.writeByte((byte) 0);
            data.writeLong(min);
        } else {
            final int bitsPerValue = DirectWriter.unsignedBitsRequired((max - min) / gcd);
            buffer.reset();
            assert buffer.size() == 0;
            final DirectWriter w = DirectWriter.getInstance(buffer, length, bitsPerValue);
            for (int i = 0; i < length; ++i) {
                w.add((values[i] - min) / gcd);
            }
            w.finish();
            data.writeByte((byte) bitsPerValue);
            data.writeLong(min);
            data.writeInt(Math.toIntExact(buffer.size()));
            buffer.copyTo(data);
        }
    }

    public void addSortedNumericField(FieldInfo field, StarTreeValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(SORTED_NUMERIC);
        doAddSortedNumericField(field, valuesProducer, false);
    }

    private void doAddSortedNumericField(FieldInfo field, StarTreeValuesProducer valuesProducer, boolean ords) throws IOException {
        long[] stats = writeValues(field, valuesProducer, ords);
        int numDocsWithField = Math.toIntExact(stats[0]);
        long numValues = stats[1];
        assert numValues >= numDocsWithField;

        meta.writeInt(numDocsWithField);
        if (numValues > numDocsWithField) {
            long start = data.getFilePointer();
            meta.writeLong(start);
            meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

            final DirectMonotonicWriter addressesWriter = DirectMonotonicWriter.getInstance(
                meta,
                data,
                numDocsWithField + 1L,
                DIRECT_MONOTONIC_BLOCK_SHIFT
            );
            long addr = 0;
            addressesWriter.add(addr);
            StarTreeSortedNumericValues values = valuesProducer.getStarTreeSortedNumericValues(field);
            for (int doc = values.nextEntry(); doc != StarTreeValuesIterator.NO_MORE_ENTRIES; doc = values.nextEntry()) {
                addr += values.docValueCount();
                addressesWriter.add(addr);
            }
            addressesWriter.finish();
            meta.writeLong(data.getFilePointer() - start);
        }
    }
}
