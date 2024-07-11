/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.index.compositeindex.datacube.startree.meta.StarTreeMetadata;

import java.io.IOException;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.MAGIC_MARKER;
import static org.opensearch.index.compositeindex.CompositeIndexConstants.VERSION;

/**
 * Off heap implementation of the star-tree.
 *
 * @opensearch.experimental
 */
public class OffHeapStarTree implements StarTree {
    private static final Logger logger = LogManager.getLogger(OffHeapStarTree.class);
    private final OffHeapStarTreeNode root;
    private final Integer numNodes;

    public OffHeapStarTree(IndexInput data, StarTreeMetadata starTreeMetadata) throws IOException {
        long magicMarker = data.readLong();
        if (MAGIC_MARKER != magicMarker) {
            logger.error("Invalid magic marker");
            throw new IOException("Invalid magic marker");
        }
        int version = data.readInt();
        if (VERSION != version) {
            logger.error("Invalid star tree version");
            throw new IOException("Invalid version");
        }
        numNodes = data.readInt(); // num nodes

        RandomAccessInput in = data.randomAccessSlice(data.getFilePointer(), starTreeMetadata.getDataLength() - data.getFilePointer());
        root = new OffHeapStarTreeNode(in, 0);
    }

    @Override
    public StarTreeNode getRoot() {
        return root;
    }

    /**
     * Returns the number of nodes in star-tree
     *
     * @return number of nodes in te star-tree
     */
    public Integer getNumNodes() {
        return numNodes;
    }

}
