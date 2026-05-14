/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.SegmentFile;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * DataFusion reader for JNI operations.
 * <p>
 * Each reader represents a point-in-time snapshot of parquet/arrow files for a shard.
 * Created from a catalog snapshot during refresh; closed when the associated catalog
 * snapshot is removed.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReader implements Closeable {

    private static final Logger logger = LogManager.getLogger(DatafusionReader.class);
    private final String directoryPath;
    private final ReaderHandle readerHandle;

    /**
     * Creates a DatafusionReader for the given shard directory and per-segment files.
     * <p>
     * Each {@link WriterFileSet} carries its writer generation and the files that make up
     * one segment. We flatten into a {@code List<SegmentFile>}: one entry per file, with
     * the segment's generation attached.
     *
     * @param directoryPath shard data directory
     * @param writerFileSets the per-segment file sets from the catalog snapshot
     */
    public DatafusionReader(String directoryPath, Collection<WriterFileSet> writerFileSets) {
        this.directoryPath = directoryPath;
        List<SegmentFile> segmentFiles;
        if (writerFileSets == null || writerFileSets.isEmpty()) {
            segmentFiles = List.of();
        } else {
            segmentFiles = new ArrayList<>();
            for (WriterFileSet wfs : writerFileSets) {
                long gen = wfs.writerGeneration();
                for (String file : wfs.files()) {
                    segmentFiles.add(new SegmentFile(gen, file));
                }
            }
        }
        readerHandle = new ReaderHandle(directoryPath, segmentFiles);
    }

    /**
     * Wraps a pre-existing native reader pointer (test only).
     * The caller retains ownership — this reader will NOT close the handle.
     */
    DatafusionReader(long nativePtr) {
        this.directoryPath = "";
        this.readerHandle = ReaderHandle.wrap(nativePtr);
    }

    @Override
    public void close() throws IOException {
        readerHandle.close();
        logger.debug("DatafusionReader closed for [{}]", directoryPath);
    }

    /**
     * Returns the type-safe handle to the native reader.
     * Callers should hold this reference and call
     * {@link ReaderHandle#getPointer()} only at JNI invocation time.
     */
    public ReaderHandle getReaderHandle() {
        return readerHandle;
    }
}
