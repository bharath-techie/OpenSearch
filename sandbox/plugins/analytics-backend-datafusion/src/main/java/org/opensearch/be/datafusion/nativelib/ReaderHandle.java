/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.analytics.backend.jni.NativeHandle;

import java.util.List;

/**
 * Type-safe handle for native reader.
 */
public final class ReaderHandle extends NativeHandle {

    private final boolean ownsPointer;

    /**
     * Creates a reader handle by allocating a native DataFusion reader for the given path
     * and {@link SegmentFile}s. Each {@link SegmentFile} pairs one parquet file with the
     * writer generation of the segment that produced it; that generation is sourced from
     * the catalog snapshot's {@code WriterFileSet.writerGeneration}.
     *
     * @param path the directory path containing data files
     * @param segmentFiles the files to read, each tagged with its writer generation
     */
    public ReaderHandle(String path, List<SegmentFile> segmentFiles) {
        super(NativeBridge.createDatafusionReader(path, segmentFiles));
        this.ownsPointer = true;
    }

    /** Wraps an existing pointer without taking ownership. */
    private ReaderHandle(long existingPtr) {
        super(existingPtr);
        this.ownsPointer = false;
    }

    @Override
    protected void doClose() {
        if (ownsPointer) {
            NativeBridge.closeDatafusionReader(ptr);
        }
    }

    /**
     * Wraps a pre-existing native pointer without taking ownership (test only).
     * @param existingPtr the native pointer to wrap
     */
    public static ReaderHandle wrap(long existingPtr) {
        return new ReaderHandle(existingPtr);
    }
}
