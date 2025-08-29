/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvDataSourceCodecTests extends OpenSearchTestCase {
    private static final Logger logger = LogManager.getLogger(CsvDataSourceCodecTests.class);

    public void testCsvSearcherSupplier() throws IOException {
        logger.info("Starting test");
        CsvDataSourceCodec codec = new CsvDataSourceCodec("/Users/abandeji/Public/work-dump/testing_supplier");
        List<String> files = new ArrayList<>();
        logger.info("output.parquet");
        // Added the files through refresh
        logger.info("Adding Registry");
        codec.registerDirectory("/Users/abandeji/Public/work-dump/testing_supplier", files, 1L).join();

        System.out.println("Added Registry");
        // Create session context
        SearcherSupplier searcherSupplier = codec.acquireSessionContextSupplier(1L, 1L);
        System.out.println("Creating Session Context");
        Searcher searcher = searcherSupplier.acquireSearcher();
        System.out.println("Acquired Session Context");
        searcher.executeSubstraitQuery(null);
        System.out.println("Executed Query");
    }
}
