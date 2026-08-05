/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Test-only stand-in for the composite engine's Lucene sidecar terms index: sorted distinct
 * terms with postings, serialized to a simple file at write time and served through the real
 * {@link Terms}/{@link TermsEnum}/{@link PostingsEnum} contract at read time. This is what
 * {@link TermDictionary} and {@link UninvertedOrdinals} rank against in production; feeding
 * them through the same abstraction lets Lucene's doc-values contract battery exercise the
 * ordinal tiers for real.
 */
final class SidecarTerms extends Terms {

    private final BytesRef[] terms;
    private final int[][] postings;
    private final long sumDocFreq;
    private final int docCount;

    private SidecarTerms(BytesRef[] terms, int[][] postings, long sumDocFreq, int docCount) {
        this.terms = terms;
        this.postings = postings;
        this.sumDocFreq = sumDocFreq;
        this.docCount = docCount;
    }

    /** Serializes sorted terms + postings. {@code termToDocs[i]} must be ascending doc ids. */
    static void write(Path file, List<BytesRef> sortedTerms, List<int[]> termToDocs) throws IOException {
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(file))) {
            out.writeInt(sortedTerms.size());
            for (int i = 0; i < sortedTerms.size(); i++) {
                BytesRef term = sortedTerms.get(i);
                out.writeInt(term.length);
                out.write(term.bytes, term.offset, term.length);
                int[] docs = termToDocs.get(i);
                out.writeInt(docs.length);
                for (int doc : docs) {
                    out.writeInt(doc);
                }
            }
        }
    }

    static SidecarTerms read(Path file) throws IOException {
        try (DataInputStream in = new DataInputStream(Files.newInputStream(file))) {
            int termCount = in.readInt();
            BytesRef[] terms = new BytesRef[termCount];
            int[][] postings = new int[termCount][];
            long sumDocFreq = 0;
            java.util.BitSet docsWithValue = new java.util.BitSet();
            for (int i = 0; i < termCount; i++) {
                byte[] bytes = new byte[in.readInt()];
                in.readFully(bytes);
                terms[i] = new BytesRef(bytes);
                int[] docs = new int[in.readInt()];
                for (int d = 0; d < docs.length; d++) {
                    docs[d] = in.readInt();
                    docsWithValue.set(docs[d]);
                }
                postings[i] = docs;
                sumDocFreq += docs.length;
            }
            return new SidecarTerms(terms, postings, sumDocFreq, docsWithValue.cardinality());
        }
    }

    @Override
    public TermsEnum iterator() {
        return new SidecarTermsEnum();
    }

    @Override
    public long size() {
        return terms.length;
    }

    @Override
    public long getSumTotalTermFreq() {
        return sumDocFreq;
    }

    @Override
    public long getSumDocFreq() {
        return sumDocFreq;
    }

    @Override
    public int getDocCount() {
        return docCount;
    }

    @Override
    public boolean hasFreqs() {
        return false;
    }

    @Override
    public boolean hasOffsets() {
        return false;
    }

    @Override
    public boolean hasPositions() {
        return false;
    }

    @Override
    public boolean hasPayloads() {
        return false;
    }

    /**
     * Deliberately mirrors BlockTree's behavior: {@code ord()} is unsupported, so consumers
     * (OrdinalMap!) must go through {@link UninvertedOrdinals}'s ord-tracking wrapper —
     * exactly the production constraint the battery should exercise.
     */
    private final class SidecarTermsEnum extends BaseTermsEnum {
        private int position = -1;

        @Override
        public BytesRef next() {
            position++;
            return position < terms.length ? terms[position] : null;
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) {
            int idx = Arrays.binarySearch(terms, text);
            if (idx >= 0) {
                position = idx;
                return SeekStatus.FOUND;
            }
            position = -idx - 1;
            return position >= terms.length ? SeekStatus.END : SeekStatus.NOT_FOUND;
        }

        @Override
        public void seekExact(long ord) {
            throw new UnsupportedOperationException("sidecar terms have no ord index (mirrors BlockTree)");
        }

        @Override
        public BytesRef term() {
            return terms[position];
        }

        @Override
        public long ord() {
            throw new UnsupportedOperationException("sidecar terms have no ord index (mirrors BlockTree)");
        }

        @Override
        public int docFreq() {
            return postings[position].length;
        }

        @Override
        public long totalTermFreq() {
            return postings[position].length;
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) {
            int[] docs = postings[position];
            return new PostingsEnum() {
                private int idx = -1;

                @Override
                public int docID() {
                    if (idx < 0) {
                        return -1;
                    }
                    return idx < docs.length ? docs[idx] : DocIdSetIterator.NO_MORE_DOCS;
                }

                @Override
                public int nextDoc() {
                    idx++;
                    return docID();
                }

                @Override
                public int advance(int target) {
                    do {
                        idx++;
                    } while (idx < docs.length && docs[idx] < target);
                    return docID();
                }

                @Override
                public long cost() {
                    return docs.length;
                }

                @Override
                public int freq() {
                    return 1;
                }

                @Override
                public int nextPosition() {
                    return -1;
                }

                @Override
                public int startOffset() {
                    return -1;
                }

                @Override
                public int endOffset() {
                    return -1;
                }

                @Override
                public BytesRef getPayload() {
                    return null;
                }
            };
        }

        @Override
        public ImpactsEnum impacts(int flags) {
            throw new UnsupportedOperationException();
        }
    }
}
