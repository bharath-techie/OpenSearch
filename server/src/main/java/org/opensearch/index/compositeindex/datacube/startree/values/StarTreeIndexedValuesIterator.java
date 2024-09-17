/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.values;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

/**
 * Copy of IndexedDISI specific for star tree
 */
public class StarTreeIndexedValuesIterator {
    // jump-table time/space trade-offs to consider:
    // The block offsets and the block indexes could be stored in more compressed form with
    // two PackedInts or two MonotonicDirectReaders.
    // The DENSE ranks (default 128 shorts = 256 bytes) could likewise be compressed. But as there is
    // at least 4096 set bits in DENSE blocks, there will be at least one rank with 2^12 bits, so it
    // is doubtful if there is much to gain here.

    private static final int BLOCK_SIZE = 65536; // The number of docIDs that a single block represents

    private static final int DENSE_BLOCK_LONGS = BLOCK_SIZE / Long.SIZE; // 1024
    public static final byte DEFAULT_DENSE_RANK_POWER = 9; // Every 512 docIDs / 8 longs

    static final int MAX_ARRAY_LENGTH = (1 << 12) - 1;

    private static void flush(int block, FixedBitSet buffer, int cardinality, byte denseRankPower, IndexOutput out) throws IOException {
        assert block >= 0 && block < BLOCK_SIZE;
        out.writeShort((short) block);
        assert cardinality > 0 && cardinality <= BLOCK_SIZE;
        out.writeShort((short) (cardinality - 1));
        if (cardinality > MAX_ARRAY_LENGTH) {
            if (cardinality != BLOCK_SIZE) { // all docs are set
                if (denseRankPower != -1) {
                    final byte[] rank = createRank(buffer, denseRankPower);
                    out.writeBytes(rank, rank.length);
                }
                for (long word : buffer.getBits()) {
                    out.writeLong(word);
                }
            }
        } else {
            BitSetIterator it = new BitSetIterator(buffer, cardinality);
            for (int doc = it.nextDoc(); doc != StarTreeValuesIterator.NO_MORE_ENTRIES; doc = it.nextDoc()) {
                out.writeShort((short) doc);
            }
        }
    }

    // Creates a DENSE rank-entry (the number of set bits up to a given point) for the buffer.
    // One rank-entry for every {@code 2^denseRankPower} bits, with each rank-entry using 2 bytes.
    // Represented as a byte[] for fast flushing and mirroring of the retrieval representation.
    private static byte[] createRank(FixedBitSet buffer, byte denseRankPower) {
        final int longsPerRank = 1 << (denseRankPower - 6);
        final int rankMark = longsPerRank - 1;
        final int rankIndexShift = denseRankPower - 7; // 6 for the long (2^6) + 1 for 2 bytes/entry
        final byte[] rank = new byte[DENSE_BLOCK_LONGS >> rankIndexShift];
        final long[] bits = buffer.getBits();
        int bitCount = 0;
        for (int word = 0; word < DENSE_BLOCK_LONGS; word++) {
            if ((word & rankMark) == 0) { // Every longsPerRank longs
                rank[word >> rankIndexShift] = (byte) (bitCount >> 8);
                rank[(word >> rankIndexShift) + 1] = (byte) (bitCount & 0xFF);
            }
            bitCount += Long.bitCount(bits[word]);
        }
        return rank;
    }

    /**
     * Writes the docIDs from it to out, in logical blocks, one for each 65536 docIDs in monotonically
     * increasing gap-less order. The caller must keep track of the number of jump-table entries
     * (returned by this method) as well as the denseRankPower and provide them when constructing an
     * IndexedDISI for reading.
     *
     * @param it the document IDs.
     * @param out destination for the blocks.
     * @param denseRankPower for {@IndexedDISI.Method#DENSE} blocks, a rank will be written every {@code
     *     2^denseRankPower} docIDs. Values &lt; 7 (every 128 docIDs) or &gt; 15 (every 32768 docIDs)
     *     disables DENSE rank. Recommended values are 8-12: Every 256-4096 docIDs or 4-64 longs.
     *     {@link #DEFAULT_DENSE_RANK_POWER} is 9: Every 512 docIDs. This should be stored in meta and
     *     used when creating an instance of IndexedDISI.
     * @throws IOException if there was an error writing to out.
     * @return the number of jump-table entries following the blocks, -1 for no entries. This should
     *     be stored in meta and used when creating an instance of IndexedDISI.
     */
    public static short writeBitSet(StarTreeValuesIterator it, IndexOutput out, byte denseRankPower) throws IOException {
        final long origo = out.getFilePointer(); // All jumps are relative to the origo
        if ((denseRankPower < 7 || denseRankPower > 15) && denseRankPower != -1) {
            throw new IllegalArgumentException(
                "Acceptable values for denseRankPower are 7-15 (every 128-32768 docIDs). "
                    + "The provided power was "
                    + denseRankPower
                    + " (every "
                    + (int) Math.pow(2, denseRankPower)
                    + " docIDs)"
            );
        }
        int totalCardinality = 0;
        int blockCardinality = 0;
        final FixedBitSet buffer = new FixedBitSet(1 << 16);
        int[] jumps = new int[ArrayUtil.oversize(1, Integer.BYTES * 2)];
        int prevBlock = -1;
        int jumpBlockIndex = 0;

        for (int doc = it.nextEntry(); doc != StarTreeValuesIterator.NO_MORE_ENTRIES; doc = it.nextEntry()) {
            final int block = doc >>> 16;
            if (prevBlock != -1 && block != prevBlock) {
                // Track offset+index from previous block up to current
                jumps = addJumps(jumps, out.getFilePointer() - origo, totalCardinality, jumpBlockIndex, prevBlock + 1);
                jumpBlockIndex = prevBlock + 1;
                // Flush block
                flush(prevBlock, buffer, blockCardinality, denseRankPower, out);
                // Reset for next block
                buffer.clear();
                totalCardinality += blockCardinality;
                blockCardinality = 0;
            }
            buffer.set(doc & 0xFFFF);
            blockCardinality++;
            prevBlock = block;
        }
        if (blockCardinality > 0) {
            jumps = addJumps(jumps, out.getFilePointer() - origo, totalCardinality, jumpBlockIndex, prevBlock + 1);
            totalCardinality += blockCardinality;
            flush(prevBlock, buffer, blockCardinality, denseRankPower, out);
            buffer.clear();
            prevBlock++;
        }
        final int lastBlock = prevBlock == -1 ? 0 : prevBlock; // There will always be at least 1 block (NO_MORE_DOCS)
        // Last entry is a SPARSE with blockIndex == 32767 and the single entry 65535, which becomes the
        // docID NO_MORE_DOCS
        // To avoid creating 65K jump-table entries, only a single entry is created pointing to the
        // offset of the
        // NO_MORE_DOCS block, with the jumpBlockIndex set to the logical EMPTY block after all real
        // blocks.
        jumps = addJumps(jumps, out.getFilePointer() - origo, totalCardinality, lastBlock, lastBlock + 1);
        buffer.set(StarTreeValuesIterator.NO_MORE_ENTRIES & 0xFFFF);
        flush(StarTreeValuesIterator.NO_MORE_ENTRIES >>> 16, buffer, 1, denseRankPower, out);
        // offset+index jump-table stored at the end
        return flushBlockJumps(jumps, lastBlock + 1, out);
    }

    // Adds entries to the offset & index jump-table for blocks
    private static int[] addJumps(int[] jumps, long offset, int index, int startBlock, int endBlock) {
        assert offset < Integer.MAX_VALUE : "Logically the offset should not exceed 2^30 but was >= Integer.MAX_VALUE";
        jumps = ArrayUtil.grow(jumps, (endBlock + 1) * 2);
        for (int b = startBlock; b < endBlock; b++) {
            jumps[b * 2] = index;
            jumps[b * 2 + 1] = (int) offset;
        }
        return jumps;
    }

    // Flushes the offset & index jump-table for blocks. This should be the last data written to out
    // This method returns the blockCount for the blocks reachable for the jump_table or -1 for no
    // jump-table
    private static short flushBlockJumps(int[] jumps, int blockCount, IndexOutput out) throws IOException {
        if (blockCount == 2) { // Jumps with a single real entry + NO_MORE_DOCS is just wasted space so we ignore
            // that
            blockCount = 0;
        }
        for (int i = 0; i < blockCount; i++) {
            out.writeInt(jumps[i * 2]); // index
            out.writeInt(jumps[i * 2 + 1]); // offset
        }
        // As there are at most 32k blocks, the count is a short
        // The jumpTableOffset will be at lastPos - (blockCount * Long.BYTES)
        return (short) blockCount;
    }
}
