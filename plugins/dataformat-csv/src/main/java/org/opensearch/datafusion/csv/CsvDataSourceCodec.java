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
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.vectorized.execution.spi.DataSourceCodec;
import org.opensearch.vectorized.execution.spi.RecordBatchStream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Datasource codec implementation for CSV files
 */
public class CsvDataSourceCodec implements DataSourceCodec {

    private static final Logger logger = LogManager.getLogger(CsvDataSourceCodec.class);
    private final String shardPath = "Shard-0";
    private static final AtomicLong runtimeIdGenerator = new AtomicLong(0);
    private static final AtomicLong sessionIdGenerator = new AtomicLong(0);
    private final ConcurrentHashMap<Long, SearcherSupplier> sessionContextSuppliers = new ConcurrentHashMap<>();
    // This should come from the Constructor? Should we move this to the DataFusionEngine?
    private final ListingReferenceManager listingReferenceManager;

    // Ideally this should be maintained by the Engine...
    private final ShardViewReferenceManager shardViewReferenceManager = new ShardViewReferenceManager();


    public CsvDataSourceCodec(String path) {
        listingReferenceManager = new ListingReferenceManager(new ListingTable(path, new String[0]));
    }

    // JNI library loading
    static {
        try {
            JniLibraryLoader.loadLibrary();
            logger.info("DataFusion JNI library loaded successfully");
        } catch (Exception e) {
            logger.error("Failed to load DataFusion JNI library", e);
            throw new RuntimeException("Failed to initialize DataFusion JNI library", e);
        }
    }

    @Override
    public CompletableFuture<Void> registerDirectory(String directoryPath, List<String> fileNames, long runtimeId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logger.debug("Registering directory: {} with {} files", directoryPath, fileNames.size());

                // We can do this as well:
                listingReferenceManager.createListingTable(directoryPath, fileNames);

                return null;
            } catch (Exception e) {
                logger.error("Failed to register directory: " + directoryPath, e);
                throw new CompletionException("Failed to register directory", e);
            }
        });
    }

    @Override
    public CompletableFuture<Long> createSessionContext(long globalRuntimeEnvId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long sessionId = sessionIdGenerator.incrementAndGet();
                logger.debug("Creating session context with ID: {} for runtime: {}", sessionId, globalRuntimeEnvId);

                // Default configuration
                String[] configKeys = { "batch_size", "target_partitions" };
                String[] configValues = { "1024", "4" };

                // Create native session context
                // We need to do this operation Atomically:

                SearcherSupplier searcherSupplier = acquireSessionContextSupplier(sessionId, globalRuntimeEnvId);
                sessionContextSuppliers.put(sessionId, searcherSupplier);

                logger.info("Created session context with ID: {}", sessionId);
                return sessionId;
            } catch (Exception e) {
                logger.error("Failed to create session context for runtime: " + globalRuntimeEnvId, e);
                throw new CompletionException("Failed to create session context", e);
            }
        });
    }

    @Override
    public CompletableFuture<RecordBatchStream> executeSubstraitQuery(long contextId, byte[] substraitPlanBytes) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logger.debug("Executing Substrait query for session: {}", contextId);

                SearcherSupplier searcherSupplier = sessionContextSuppliers.get(contextId);
                if (searcherSupplier == null ) {
                    throw new IllegalArgumentException("Invalid session context ID: " + contextId);
                }

                Searcher searcher = searcherSupplier.acquireSearcher();
                if (searcher == null) {
                    throw new IllegalStateException("Failed to acquire session context");
                }

                long nativeStreamPtr = searcher.executeSubstraitQuery(substraitPlanBytes);
                if (nativeStreamPtr == 0) {
                    throw new RuntimeException("Failed to execute Substrait query");
                }

                // Create Java wrapper for the native stream
                RecordBatchStream stream = new CsvRecordBatchStream(nativeStreamPtr);

                logger.info("Successfully executed Substrait query for session: {}", contextId);
                return stream;
            } catch (Exception e) {
                logger.error("Failed to execute Substrait query for session: " + contextId, e);
                throw new CompletionException("Failed to execute Substrait query", e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> closeSessionContext(long sessionContextId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logger.debug("Closing session context: {}", sessionContextId);

                SearcherSupplier searcherSupplier = sessionContextSuppliers.remove(sessionContextId);
                if (searcherSupplier != null) {
                    searcherSupplier.close();
                    logger.info("Successfully closed session context: {}", sessionContextId);
                } else {
                    logger.warn("Session context not found: {}", sessionContextId);
                }

                return null;
            } catch (Exception e) {
                logger.error("Failed to close session context: " + sessionContextId, e);
                throw new CompletionException("Failed to close session context", e);
            }
        });
    }

    public SearcherSupplier acquireSessionContextSupplier(long contextId, long globalRunTimeId) {
        try {
            ShardView  shardView = shardViewReferenceManager.acquireShardView(this.shardPath);
            SearcherSupplier reader = new SearcherSupplier() {

                @Override
                protected Searcher acquireSearcherInternal() {
                    return new Searcher(
                        contextId,
                        shardView,
                        globalRunTimeId,
                        () -> { }
                    );
                }

                @Override
                protected void doClose() {
                    try {
                        shardViewReferenceManager.release(shardView);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Failed to close", e);
                    } catch (AlreadyClosedException e) {
                        throw new AssertionError(e);
                    }
                }
            };

            return reader;
        } catch (AlreadyClosedException e) {
            throw e;
        } catch (Exception e) {
            // Should we close the engine?
        }

        return null;
    }

    // Native method declarations - these will be implemented in the JNI library
    private static native long nativeRegisterDirectory(String tableName, String directoryPath, String[] files, long runtimeId);




    private static native void nativeCloseSessionContext(long sessionContextPtr);
}
