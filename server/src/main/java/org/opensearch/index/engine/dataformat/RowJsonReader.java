/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;

/**
 * Optional capability of an {@link IndexingExecutionEngine}: reads the rows of a
 * {@link WriterFileSet} it produced back as JSON.
 *
 * <p>Used by the composite engine's materialized-view merge path: after the primary
 * format's aggregating merge folds N rows into M, the folded rows are read back through
 * this interface and re-driven through the document mapper to rebuild every secondary
 * format's segment with the exact same M rows — restoring cross-format row parity.
 *
 * <p>Values are rendered in source-compatible form: binary columns as base64 strings,
 * timestamps as epoch milliseconds. Column names are the stored (physical) names.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface RowJsonReader {

    /**
     * Reads all rows of the given file set as a JSON array of objects (one object per
     * row, in row-id order; keys are column names).
     *
     * @param fileSet the file set to read, previously produced by this engine
     * @return JSON array string of row objects
     * @throws IOException if the files cannot be read or contain unsupported types
     */
    String readRowsAsJson(WriterFileSet fileSet) throws IOException;
}
