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
import org.opensearch.vectorized.execution.spi.DataSourceCodec;
import org.opensearch.vectorized.execution.spi.RecordBatchStream;

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
    private static final AtomicLong runtimeIdGenerator = new AtomicLong(0);
    private static final AtomicLong sessionIdGenerator = new AtomicLong(0);
    private final ConcurrentHashMap<Long, SessionContext> sessionContexts = new ConcurrentHashMap<>();
    private ListingTableManager listingTableManager = ListingTableManager.getInstance();
    // Currently I'm mapping contextID --> sessionContext and contextID --> ListingTable


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
                listingTableManager.createListingTable(directoryPath, fileNames);

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

                SessionContext sessionContext = new SessionContext(sessionId);
                sessionContexts.put(sessionId, sessionContext);

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

                SessionContext sessionContext = sessionContexts.get(contextId);
                if (sessionContext == null || sessionContext.isClosed()) {
                    throw new IllegalArgumentException("Invalid session context ID: " + contextId);
                }

                sessionContext.attachListingTable();
                long nativeStreamPtr = sessionContext.executeSubstraitQuery(substraitPlanBytes);
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

                SessionContext sessionContext = sessionContexts.remove(sessionContextId);
                if (sessionContext != null) {
                    sessionContext.close();
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

    // Native method declarations - these will be implemented in the JNI library
    private static native long nativeRegisterDirectory(String tableName, String directoryPath, String[] files, long runtimeId);




    private static native void nativeCloseSessionContext(long sessionContextPtr);
}
