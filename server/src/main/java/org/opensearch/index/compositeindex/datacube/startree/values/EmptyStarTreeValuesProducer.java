/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.values;

import org.apache.lucene.index.FieldInfo;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Copy of EmptyDocValuesProducer specific for star tree
 */
@ExperimentalApi
public class EmptyStarTreeValuesProducer extends StarTreeValuesProducer {
    protected EmptyStarTreeValuesProducer() {}

    @Override
    public StarTreeNumericValues getStarTreeNumericValues(FieldInfo field) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StarTreeSortedNumericValues getStarTreeSortedNumericValues(FieldInfo field) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkIntegrity() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException();
    }
}
