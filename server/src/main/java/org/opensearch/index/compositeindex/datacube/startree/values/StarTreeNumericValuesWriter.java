/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.values;

import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Copy of NumericDocValuesWriter.
 * This just changes the iterators from DISI to star tree specific ones
 */
@ExperimentalApi
public class StarTreeNumericValuesWriter {
    private final PackedLongValues.Builder pending;
    private PackedLongValues finalValues;
    private final Counter iwBytesUsed;
    private long bytesUsed;
    private DocsWithFieldSet docsWithField;
    private final FieldInfo fieldInfo;
    private int lastDocID = -1;

    StarTreeNumericValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
        pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
        docsWithField = new DocsWithFieldSet();
        bytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
        this.fieldInfo = fieldInfo;
        this.iwBytesUsed = iwBytesUsed;
        iwBytesUsed.addAndGet(bytesUsed);
    }

    public void addValue(int docID, long value) {
        if (docID <= lastDocID) {
            throw new IllegalArgumentException(
                "DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)"
            );
        }

        pending.add(value);
        docsWithField.add(docID);

        updateBytesUsed();

        lastDocID = docID;
    }

    private void updateBytesUsed() {
        final long newBytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
        iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
        bytesUsed = newBytesUsed;
    }

    StarTreeNumericValues getDocValues() {
        if (finalValues == null) {
            finalValues = pending.build();
        }
        return new StarTreeNumericValuesWriter.BufferedNumericDocValues(finalValues, docsWithField.iterator());
    }

    static StarTreeNumericValuesWriter.NumericDVs sortDocValues(
        int maxDoc,
        Sorter.DocMap sortMap,
        StarTreeNumericValues oldDocValues,
        boolean dense
    ) throws IOException {
        FixedBitSet docsWithField = null;
        if (dense == false) {
            docsWithField = new FixedBitSet(maxDoc);
        }

        long[] values = new long[maxDoc];
        while (true) {
            int docID = oldDocValues.nextEntry();
            if (docID == NO_MORE_DOCS) {
                break;
            }
            int newDocID = sortMap.oldToNew(docID);
            if (docsWithField != null) {
                docsWithField.set(newDocID);
            }
            values[newDocID] = oldDocValues.longValue();
        }
        return new StarTreeNumericValuesWriter.NumericDVs(values, docsWithField);
    }

    public void flush(SegmentWriteState state, Sorter.DocMap sortMap, StarTree99ValuesConsumer dvConsumer) throws IOException {
        if (finalValues == null) {
            finalValues = pending.build();
        }

        dvConsumer.addNumericField(fieldInfo, getDocValuesProducer(fieldInfo, finalValues, docsWithField, sortMap));
    }

    static StarTreeValuesProducer getDocValuesProducer(
        FieldInfo writerFieldInfo,
        PackedLongValues values,
        DocsWithFieldSet docsWithField,
        Sorter.DocMap sortMap
    ) throws IOException {
        final StarTreeNumericValuesWriter.NumericDVs sorted;
        if (sortMap != null) {
            StarTreeNumericValues oldValues = new StarTreeNumericValuesWriter.BufferedNumericDocValues(values, docsWithField.iterator());
            sorted = sortDocValues(sortMap.size(), sortMap, oldValues, sortMap.size() == docsWithField.cardinality());
        } else {
            sorted = null;
        }

        return new EmptyStarTreeValuesProducer() {
            @Override
            public StarTreeNumericValues getStarTreeNumericValues(FieldInfo fieldInfo) {
                if (fieldInfo != writerFieldInfo) {
                    throw new IllegalArgumentException("wrong fieldInfo");
                }
                if (sorted == null) {
                    return new BufferedNumericDocValues(values, docsWithField.iterator());
                } else {
                    return new SortingNumericDocValues(sorted);
                }
            }
        };
    }

    // iterates over the values we have in ram
    static class BufferedNumericDocValues extends StarTreeNumericValues {
        final PackedLongValues.Iterator iter;
        final DocIdSetIterator docsWithField;
        private long value;

        BufferedNumericDocValues(PackedLongValues values, DocIdSetIterator docsWithFields) {
            this.iter = values.iterator();
            this.docsWithField = docsWithFields;
        }

        @Override
        public int entryId() {
            return docsWithField.docID();
        }

        @Override
        public int nextEntry() throws IOException {
            int docID = docsWithField.nextDoc();
            if (docID != NO_MORE_ENTRIES) {
                value = iter.next();
            }
            return docID;
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            return docsWithField.cost();
        }

        @Override
        public long longValue() {
            return value;
        }
    }

    static class SortingNumericDocValues extends StarTreeNumericValues {

        private final NumericDVs dvs;
        private int docID = -1;
        private long cost = -1;

        SortingNumericDocValues(NumericDVs dvs) {
            this.dvs = dvs;
        }

        @Override
        public int entryId() {
            return docID;
        }

        @Override
        public int nextEntry() {
            if (docID + 1 == dvs.maxDoc()) {
                docID = NO_MORE_ENTRIES;
            } else {
                docID = dvs.advance(docID + 1);
            }
            return docID;
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException("use nextDoc() instead");
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            // needed in IndexSorter#{Long|Int|Double|Float}Sorter
            docID = target;
            return dvs.advanceExact(target);
        }

        @Override
        public long longValue() {
            return dvs.values[docID];
        }

        @Override
        public long cost() {
            if (cost == -1) {
                cost = dvs.cost();
            }
            return cost;
        }
    }

    static class NumericDVs {
        private final long[] values;
        private final BitSet docsWithField;
        private final int maxDoc;

        NumericDVs(long[] values, BitSet docsWithField) {
            this.values = values;
            this.docsWithField = docsWithField;
            this.maxDoc = values.length;
        }

        int maxDoc() {
            return maxDoc;
        }

        private boolean advanceExact(int target) {
            if (docsWithField != null) {
                return docsWithField.get(target);
            }
            return true;
        }

        private int advance(int target) {
            if (docsWithField != null) {
                return docsWithField.nextSetBit(target);
            }

            // Only called when target is less than maxDoc
            return target;
        }

        private long cost() {
            if (docsWithField != null) {
                return docsWithField.cardinality();
            }
            return maxDoc;
        }
    }
}
