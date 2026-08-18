/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * input data for a merge operation.
 * Use {@link Builder} to construct instances.
 *
 * <p>When {@code replacementFileSet} is non-null, the merge must consume the input
 * {@code segments} but produce a merged segment whose content is exactly the rows of the
 * replacement file set (already written by a writer of this format at
 * {@code newWriterGeneration}), rather than the concatenation of the input segments.
 * This is used by the composite engine's materialized-view merge path, where the primary
 * format's aggregating merge folds rows and each secondary segment is rebuilt 1:1 from
 * the folded output.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record MergeInput(List<Segment> segments, RowIdMapping rowIdMapping, long newWriterGeneration, WriterFileSet replacementFileSet) {

    public MergeInput {
        segments = List.copyOf(segments);
    }

    public MergeInput(List<Segment> segments, RowIdMapping rowIdMapping, long newWriterGeneration) {
        this(segments, rowIdMapping, newWriterGeneration, null);
    }

    private MergeInput(Builder builder) {
        this(new ArrayList<>(builder.segments), builder.rowIdMapping, builder.newWriterGeneration, builder.replacementFileSet);
    }

    /**
     * Returns the {@link WriterFileSet} for the given data format from each segment.
     *
     * @param formatName the data format name (e.g. "parquet")
     * @return list of writer file sets for the format across all segments
     */
    public List<WriterFileSet> getFilesForFormat(String formatName) {
        return segments.stream().map(seg -> seg.dfGroupedSearchableFiles().get(formatName)).filter(Objects::nonNull).toList();
    }

    /**
     * Returns a new builder for constructing {@link MergeInput} instances.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link MergeInput}.
     */
    @ExperimentalApi
    public static class Builder {
        private List<Segment> segments = new ArrayList<>();
        private RowIdMapping rowIdMapping;
        private long newWriterGeneration;
        private WriterFileSet replacementFileSet;

        private Builder() {}

        /**
         * Sets the list of segments to merge.
         *
         * @param segments the segments to merge
         * @return this builder
         */
        public Builder segments(List<Segment> segments) {
            this.segments = new ArrayList<>(segments);
            return this;
        }

        /**
         * Adds a segment to merge.
         *
         * @param segment the segment to add
         * @return this builder
         */
        public Builder addSegment(Segment segment) {
            this.segments.add(segment);
            return this;
        }

        /**
         * Sets the row ID mapping for secondary data format merges.
         *
         * @param rowIdMapping the row ID mapping
         * @return this builder
         */
        public Builder rowIdMapping(RowIdMapping rowIdMapping) {
            this.rowIdMapping = rowIdMapping;
            return this;
        }

        /**
         * Sets the writer generation for the merged output.
         *
         * @param newWriterGeneration the new writer generation
         * @return this builder
         */
        public Builder newWriterGeneration(long newWriterGeneration) {
            this.newWriterGeneration = newWriterGeneration;
            return this;
        }

        /**
         * Sets the replacement content for the merged segment (see class javadoc).
         *
         * @param replacementFileSet flushed file set whose rows become the merged segment's content
         * @return this builder
         */
        public Builder replacementFileSet(WriterFileSet replacementFileSet) {
            this.replacementFileSet = replacementFileSet;
            return this;
        }

        /**
         * Builds an immutable {@link MergeInput}.
         *
         * @return the constructed MergeInput
         */
        public MergeInput build() {
            return new MergeInput(this);
        }
    }
}
