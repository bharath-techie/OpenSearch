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
 * Metadata for a file.
 * 
 * @param df the data format
 * @param fileName the file name
 */
@ExperimentalApi
public record FileMetadata(DataFormat df, String fileName) { }
