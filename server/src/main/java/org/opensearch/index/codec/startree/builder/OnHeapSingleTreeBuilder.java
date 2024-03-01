/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.index.codec.startree.builder;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.codec.startree.codec.StarTreeAggregatedValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** On heap single tree builder
 * This is not well tested - as initial benchmarks
 * */
public class OnHeapSingleTreeBuilder extends BaseSingleTreeBuilder {
    private final List<Record> _records = new ArrayList<>();

    public OnHeapSingleTreeBuilder(
        IndexOutput output,
        List<String> dimensionsSplitOrder,
        Map<String, SortedNumericDocValues> docValuesMap,
        int maxDoc,
        DocValuesConsumer consumer,
        SegmentWriteState state
    ) throws IOException {
        super(output, dimensionsSplitOrder, docValuesMap, maxDoc, consumer, state);
    }

    @Override
    public void build(List<StarTreeAggregatedValues> aggrList) throws IOException {
        build(mergeRecords(aggrList), true);
    }

    private Iterator<Record> mergeRecords(List<StarTreeAggregatedValues> aggrList) throws IOException {
        List<BaseSingleTreeBuilder.Record> records = new ArrayList<>();
        for (StarTreeAggregatedValues starTree : aggrList) {
            boolean endOfDoc = false;
            while (!endOfDoc) {
                long[] dims = new long[starTree.dimensionValues.size()];
                int i = 0;
                for (Map.Entry<String, SortedNumericDocValues> dimValue : starTree.dimensionValues.entrySet()) {
                    int doc = dimValue.getValue().nextDoc();
                    long val = dimValue.getValue().nextValue();
                    endOfDoc = doc == DocIdSetIterator.NO_MORE_DOCS || val == -1;
                    if (endOfDoc) {
                        break;
                    }
                    dims[i] = val;
                    i++;
                }
                if (endOfDoc) {
                    break;
                }
                i = 0;
                Object[] metrics = new Object[starTree.metricValues.size()];
                for (Map.Entry<String, SortedNumericDocValues> metricValue : starTree.metricValues.entrySet()) {
                    metricValue.getValue().nextDoc();
                    metrics[i] = metricValue.getValue().nextValue();
                    i++;
                }
                BaseSingleTreeBuilder.Record record = new BaseSingleTreeBuilder.Record(dims, metrics);
                records.add(record);
            }
        }
        BaseSingleTreeBuilder.Record[] recordsArr = new BaseSingleTreeBuilder.Record[records.size()];
        records.toArray(recordsArr);
        records = null;
        return sortRecords(recordsArr);
    }

    @Override
    void appendRecord(Record record) throws IOException {
        _records.add(record);
    }

    @Override
    Record getStarTreeRecord(int docId) throws IOException {
        return _records.get(docId);
    }

    @Override
    long getDimensionValue(int docId, int dimensionId) throws IOException {
        return _records.get(docId)._dimensions[dimensionId];
    }

    @Override
    Iterator<Record> sortAndAggregateSegmentRecords(int numDocs) throws IOException {
        Record[] records = new Record[numDocs];
        for (int i = 0; i < numDocs; i++) {
            records[i] = getNextSegmentRecord();
        }
        return sortAndAggregateSegmentRecords(records);
    }

    public Iterator<Record> sortAndAggregateSegmentRecords(Record[] records) throws IOException {
        Arrays.sort(records, (o1, o2) -> {
            for (int i = 0; i < _numDimensions; i++) {
                if (o1._dimensions[i] != o2._dimensions[i]) {
                    return Math.toIntExact(o1._dimensions[i] - o2._dimensions[i]);
                }
            }
            return 0;
        });
        return sortRecords(records);
    }

    private Iterator<Record> sortRecords(Record[] records) {
        return new Iterator<Record>() {
            boolean _hasNext = true;
            Record _currentRecord = records[0];
            int _docId = 1;

            @Override
            public boolean hasNext() {
                return _hasNext;
            }

            @Override
            public Record next() {
                Record next = mergeSegmentRecord(null, _currentRecord);
                while (_docId < records.length) {
                    Record record = records[_docId++];
                    if (!Arrays.equals(record._dimensions, next._dimensions)) {
                        _currentRecord = record;
                        return next;
                    } else {
                        next = mergeSegmentRecord(next, record);
                    }
                }
                _hasNext = false;
                return next;
            }
        };
    }

    @Override
    Iterator<Record> generateRecordsForStarNode(int startDocId, int endDocId, int dimensionId) throws IOException {
        int numDocs = endDocId - startDocId;
        Record[] records = new Record[numDocs];
        for (int i = 0; i < numDocs; i++) {
            records[i] = getStarTreeRecord(startDocId + i);
        }
        Arrays.sort(records, (o1, o2) -> {
            for (int i = dimensionId + 1; i < _numDimensions; i++) {
                if (o1._dimensions[i] != o2._dimensions[i]) {
                    return Math.toIntExact(o1._dimensions[i] - o2._dimensions[i]);
                }
            }
            return 0;
        });
        return new Iterator<Record>() {
            boolean _hasNext = true;
            Record _currentRecord = records[0];
            int _docId = 1;

            private boolean hasSameDimensions(Record record1, Record record2) {
                for (int i = dimensionId + 1; i < _numDimensions; i++) {
                    if (record1._dimensions[i] != record2._dimensions[i]) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean hasNext() {
                return _hasNext;
            }

            @Override
            public Record next() {
                Record next = mergeStarTreeRecord(null, _currentRecord);
                next._dimensions[dimensionId] = STAR_IN_DOC_VALUES_INDEX;
                while (_docId < numDocs) {
                    Record record = records[_docId++];
                    if (!hasSameDimensions(record, _currentRecord)) {
                        _currentRecord = record;
                        return next;
                    } else {
                        next = mergeStarTreeRecord(next, record);
                    }
                }
                _hasNext = false;
                return next;
            }
        };
    }
}
