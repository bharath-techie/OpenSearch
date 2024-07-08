/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import java.util.Comparator;

/** Int comparator */
public interface IntComparator extends Comparator<Integer> {
    int compare(int var1, int var2);

    @Override
    default int compare(Integer ok1, Integer ok2) {
        return this.compare(ok1, ok2);
    }
}
