/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

public class CsvDataSourceCodecTests extends OpenSearchTestCase {

    public void testCsvSearcherSupplier() {
        CsvDataSourceCodec codec = new CsvDataSourceCodec("/Users/abandeji/Public/work-dump/testing_supplier");
        List<String> files = new ArrayList<>();
        files.add("output.parquet");
        // Added the files through refresh
        codec.registerDirectory("/Users/abandeji/Public/work-dump/testing_supplier", files, 1L).join();

        // Create session context
        SearcherSupplier sp = codec.acquireSessionContextSupplier(1L, 1L);
        Searcher searcher = sp.acquireSearcher();

        searcher.executeSubstraitQuery(null);
    }
}
