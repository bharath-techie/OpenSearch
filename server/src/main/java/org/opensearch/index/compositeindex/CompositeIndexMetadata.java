/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.compositeindex.datacube.startree.meta.StarTreeMetadata;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.IOException;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.MAGIC_MARKER;
import static org.opensearch.index.compositeindex.CompositeIndexConstants.VERSION;

/**
 * This class represents the metadata of a Composite Index, which includes information about
 * the composite field name, type, and the specific metadata for the type of composite field
 * (e.g., StarTree metadata).
 *
 * @opensearch.experimental
 */
public class CompositeIndexMetadata {

    private static final Logger logger = LogManager.getLogger(CompositeIndexMetadata.class);
    private final String compositeFieldName;
    private final CompositeMappedFieldType.CompositeFieldType compositeFieldType;
    private final StarTreeMetadata starTreeMetadata;

    /**
     * Constructs a CompositeIndexMetadata object from the provided IndexInput and magic marker.
     *
     * @param meta        the IndexInput containing the metadata
     * @param magicMarker the magic marker value
     * @throws IOException if an I/O error occurs while reading the metadata
     */
    public CompositeIndexMetadata(IndexInput meta, long magicMarker) throws IOException {
        if (MAGIC_MARKER != magicMarker) {
            logger.error("Invalid composite field magic marker");
            throw new IOException("Invalid composite field magic marker");
        }
        int version = meta.readInt();
        if (VERSION != version) {
            logger.error("Invalid composite field version");
            throw new IOException("Invalid composite field version");
        }

        compositeFieldName = meta.readString();
        compositeFieldType = CompositeMappedFieldType.CompositeFieldType.fromName(meta.readString());

        switch (compositeFieldType) {
            // support for type of composite fields can be added in the future.
            case STAR_TREE:
                starTreeMetadata = new StarTreeMetadata(meta, compositeFieldName, compositeFieldType.getName());
                break;
            default:
                throw new CorruptIndexException("Invalid composite field type present in the file", meta);
        }

    }

    /**
     * Returns the star-tree metadata.
     *
     * @return the StarTreeMetadata
     */
    public StarTreeMetadata getStarTreeMetadata() {
        return starTreeMetadata;
    }

    /**
     * Returns the name of the composite field.
     *
     * @return the composite field name
     */
    public String getCompositeFieldName() {
        return compositeFieldName;
    }

    /**
     * Returns the type of the composite field.
     *
     * @return the composite field type
     */
    public CompositeMappedFieldType.CompositeFieldType getCompositeFieldType() {
        return compositeFieldType;
    }
}
