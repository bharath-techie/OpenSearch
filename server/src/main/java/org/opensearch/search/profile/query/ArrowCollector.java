/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.lucene.search.ScoreMode.TOP_DOCS;

public class ArrowCollector extends StreamCollector {

    private VectorSchemaRoot root;
    private final Schema schema;
    private VarCharVector joinFieldVector;
    private IntVector docIDVector;
    private final int batchSize;
    private int currentRow;

    public ArrowCollector(Collector in, List<FieldInfo> fields, int batchSize) {
        super(in, batchSize);
        Field docIDField = new Field("docID", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field joinField = new Field("joinField", FieldType.nullable(new ArrowType.Utf8()), null);
        schema = new Schema(asList(docIDField, joinField));
        this.batchSize = batchSize;
        this.currentRow = 0;
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot(BufferAllocator allocator) {
        this.root = VectorSchemaRoot.create(schema, allocator);
        joinFieldVector = (VarCharVector) root.getVector("joinField");
        docIDVector = (IntVector) root.getVector("docID");
        root.allocateNew();
        return root;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        LeafCollector innerLeafCollector = super.getLeafCollector(context);
        root.getFieldVectors();
        SortedSetDocValues docValues = context.reader().getSortedSetDocValues("joinField");
        return new LeafCollector() {

            @Override
            public void setScorer(Scorable scorable) throws IOException {
                innerLeafCollector.setScorer(scorable);
            }

            @Override
            public void collect(int docId) throws IOException {
                innerLeafCollector.collect(docId);
                if (docValues != null) {
                    if (docValues.advanceExact(docId)) {
                        docIDVector.set(currentRow, docId);
                        joinFieldVector.set(currentRow, docValues.lookupOrd(docValues.nextOrd()).bytes);
                        currentRow++;
                    }
                }
            }
        };
    }

    @Override
    public void onNewBatch() {
        docIDVector.allocateNew(batchSize);
        joinFieldVector.allocateNew(batchSize);
    }

    @Override
    public ScoreMode scoreMode() {
        return TOP_DOCS;
    }

    public void setWeight(Weight weight) {
        if (in != null) {
            in.setWeight(weight);
        }
    }
}
