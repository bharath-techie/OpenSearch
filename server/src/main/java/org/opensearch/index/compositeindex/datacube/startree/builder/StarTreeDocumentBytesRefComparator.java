/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class StarTreeDocumentBytesRefComparator implements Comparator<BytesRef> {
    private static final long serialVersionUID = 1L;
    private final int numDimensions;

    public StarTreeDocumentBytesRefComparator(int numDimensions) {
        this.numDimensions = numDimensions;
    }

    @Override
    public int compare(BytesRef bytesRef1, BytesRef bytesRef2) {
        if (bytesRef1 == null && bytesRef2 == null) {
            return 0;
        } else if (bytesRef1 == null) {
            return -1;
        } else if (bytesRef2 == null) {
            return 1;
        }

        byte[] bytes1 = bytesRef1.bytes;
        byte[] bytes2 = bytesRef2.bytes;
        int offset1 = bytesRef1.offset;
        int offset2 = bytesRef2.offset;

        for (int i = 0; i < numDimensions; i++) {
            long dim1 = ByteBuffer.wrap(bytes1, offset1, Long.BYTES).getLong();
            long dim2 = ByteBuffer.wrap(bytes2, offset2, Long.BYTES).getLong();

            if (dim1 == Long.MAX_VALUE && dim2 == Long.MAX_VALUE) {
                offset1 += Long.BYTES;
                offset2 += Long.BYTES;
                continue;
            } else if (dim1 == Long.MAX_VALUE) {
                return -1;
            } else if (dim2 == Long.MAX_VALUE) {
                return 1;
            } else {
                int result = Long.compare(dim1, dim2);
                if (result != 0) {
                    return result;
                }
                offset1 += Long.BYTES;
                offset2 += Long.BYTES;
            }
        }

        return 0;
    }
}
