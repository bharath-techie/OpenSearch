/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.engine.read;

public class ParquetFileFormat implements FileFormat {
    long parquetFileFormatPtr;

    ParquetFileFormat() {
        parquetFileFormatPtr = createParquetFileFormatPtr();
    }

    @Override
    public long getPointer() {
        return parquetFileFormatPtr;
    }

    native static long createParquetFileFormatPtr();
}
