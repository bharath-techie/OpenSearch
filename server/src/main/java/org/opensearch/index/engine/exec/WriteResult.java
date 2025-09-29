/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Result of a write operation.
 * 
 * @param success whether the write was successful
 * @param e exception if write failed
 * @param version document version
 * @param term document term
 * @param seqNo sequence number
 */
@ExperimentalApi
public record WriteResult(boolean success, Exception e, long version, long term, long seqNo) {
}
