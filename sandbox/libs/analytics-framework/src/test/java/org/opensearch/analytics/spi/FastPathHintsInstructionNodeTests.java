/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class FastPathHintsInstructionNodeTests extends OpenSearchTestCase {

    private static FastPathHintSpec countOnly() {
        return FastPathHintSpec.countOnly(true, FastPathHintSpec.RangeUnit.MILLIS, 1000L, 2000L);
    }

    public void testWireRoundtrip() throws Exception {
        FastPathHintsInstructionNode original = new FastPathHintsInstructionNode(countOnly());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                FastPathHintsInstructionNode decoded = new FastPathHintsInstructionNode(in);
                assertEquals(countOnly(), decoded.getHints());
                assertEquals(InstructionType.FAST_PATH_HINTS, decoded.type());
            }
        }
    }

    public void testNoneRoundtrip() throws Exception {
        FastPathHintsInstructionNode original = new FastPathHintsInstructionNode(FastPathHintSpec.NONE);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(FastPathHintSpec.NONE, new FastPathHintsInstructionNode(in).getHints());
            }
        }
    }

    /** A null spec falls back to {@link FastPathHintSpec#NONE}. */
    public void testNullSpecDefaultsToNone() {
        assertEquals(FastPathHintSpec.NONE, new FastPathHintsInstructionNode((FastPathHintSpec) null).getHints());
    }

    public void testInstructionTypeReadNodeDispatch() throws Exception {
        FastPathHintsInstructionNode node = new FastPathHintsInstructionNode(countOnly());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            node.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                InstructionNode decoded = InstructionType.FAST_PATH_HINTS.readNode(in);
                assertTrue(decoded instanceof FastPathHintsInstructionNode);
                assertEquals(countOnly(), ((FastPathHintsInstructionNode) decoded).getHints());
            }
        }
    }
}
