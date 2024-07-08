/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

/** Functional interface for swapper */
@FunctionalInterface
public interface Swapper {
    void swap(int var1, int var2);
}
