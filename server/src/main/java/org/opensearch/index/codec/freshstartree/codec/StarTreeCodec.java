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
package org.opensearch.index.codec.freshstartree.codec;

import java.util.Objects;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;


/** Codec for performing aggregation during indexing */
public class StarTreeCodec extends Codec {
    private Codec lucene95Codec;
    public static final String LUCENE_95 = "Lucene95"; // Lucene Codec to be used

    public static final String STAR_TREE_CODEC_NAME = "StarTreeCodec";

    private final DocValuesFormat dvFormat = new StarTreeDocValuesFormat();

    private final StoredFieldsFormat storedFieldsFormat;

    public StarTreeCodec() {
        super(STAR_TREE_CODEC_NAME);
        storedFieldsFormat =  new Lucene90StoredFieldsFormat(Lucene90StoredFieldsFormat.Mode.BEST_SPEED);
    }

    public Codec getDelegate() {
        if (lucene95Codec == null) {
            lucene95Codec = Codec.forName(LUCENE_95);
        }
        return lucene95Codec;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return getDelegate().postingsFormat();
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return dvFormat;
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return getDelegate().storedFieldsFormat();
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return getDelegate().termVectorsFormat(); // or getDefault()
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return getDelegate().fieldInfosFormat();
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return getDelegate().segmentInfoFormat();
    }

    @Override
    public NormsFormat normsFormat() {
        return getDelegate().normsFormat();
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return getDelegate().liveDocsFormat();
    }

    @Override
    public CompoundFormat compoundFormat() {
        return getDelegate().compoundFormat();
    }

    @Override
    public PointsFormat pointsFormat() {
        return getDelegate().pointsFormat();
    }

    @Override
    public KnnVectorsFormat knnVectorsFormat() {
        return getDelegate().knnVectorsFormat();
    }

    /**
     * TODO : change this - this is a hack
     */
    public PostingsFormat getPostingsFormatForField(String field) {
        return getDelegate().postingsFormat();
    }

    /**
     * TODO : change this - this is a hack
     */
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return docValuesFormat();
    }
}
