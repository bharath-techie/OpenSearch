/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.merge;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.util.Bits;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.List;

import static org.opensearch.be.lucene.index.LuceneWriter.WRITER_GENERATION_ATTRIBUTE;

/**
 * A {@link MergePolicy.OneMerge} whose output is exactly the content of a pre-built
 * replacement segment rather than the concatenation of the input segments.
 *
 * <p>Used by the materialized-view merge path: the primary format's aggregating merge
 * folds N rows into M new rows, and the Lucene secondary's merged segment must hold
 * exactly those M rows (rebuilt from the folded output, with new row IDs {@code 0..M-1}).
 * The first input segment's reader is substituted with the replacement segment's reader;
 * all remaining input readers are wrapped fully-deleted. Lucene's internal merge
 * transaction then atomically removes the input segments and installs a merged segment
 * whose content is the replacement — the replacement segment itself never enters the
 * shared writer.
 *
 * <p>Also stamps the writer generation attribute on the merged segment (same mechanism
 * as {@link RowIdRemappingOneMerge}).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
class ReplacementContentOneMerge extends MergePolicy.OneMerge {

    private final CodecReader replacement;
    private final long outputWriterGeneration;
    private boolean substituted;

    /**
     * @param segments               the input segments to consume
     * @param replacement            reader over the pre-built replacement segment (caller manages lifecycle)
     * @param outputWriterGeneration generation stamped on the merged output segment
     */
    ReplacementContentOneMerge(List<SegmentCommitInfo> segments, CodecReader replacement, long outputWriterGeneration) {
        super(segments);
        this.replacement = replacement;
        this.outputWriterGeneration = outputWriterGeneration;
    }

    @Override
    public CodecReader wrapForMerge(CodecReader reader) throws IOException {
        if (substituted == false) {
            substituted = true;
            return replacement;
        }
        return allDeleted(super.wrapForMerge(reader));
    }

    @Override
    public void setMergeInfo(SegmentCommitInfo info) {
        super.setMergeInfo(info);
        if (info != null) {
            info.info.putAttribute(WRITER_GENERATION_ATTRIBUTE, String.valueOf(outputWriterGeneration));
        }
    }

    /** Wraps a reader so the merge sees none of its documents. */
    private static CodecReader allDeleted(CodecReader in) {
        return new FilterCodecReader(in) {
            @Override
            public Bits getLiveDocs() {
                return new Bits.MatchNoBits(in.maxDoc());
            }

            @Override
            public int numDocs() {
                return 0;
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return in.getCoreCacheHelper();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }
}
