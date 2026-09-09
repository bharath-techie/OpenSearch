/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Instruction node carrying the planner's {@link FastPathHintSpec} for a shard fragment.
 *
 * <p>Mirrors {@code SETUP_PARTIAL_AGGREGATE}: the coordinator appends it to the instruction list
 * next to the shard-scan node, and the data node ({@code AnalyticsSearchService}) stamps the spec
 * onto {@code ShardScanExecutionContext} before the shard-scan handler reads it. Only emitted for
 * backends that consume hints (the datafusion backend), and only when the hint is non-NONE.
 *
 * @opensearch.internal
 */
public class FastPathHintsInstructionNode implements InstructionNode {

    private final FastPathHintSpec hints;

    public FastPathHintsInstructionNode(FastPathHintSpec hints) {
        this.hints = hints != null ? hints : FastPathHintSpec.NONE;
    }

    public FastPathHintsInstructionNode(StreamInput in) throws IOException {
        this.hints = new FastPathHintSpec(in);
    }

    /** The planner fast-path hints; never null. */
    public FastPathHintSpec getHints() {
        return hints;
    }

    @Override
    public InstructionType type() {
        return InstructionType.FAST_PATH_HINTS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        hints.writeTo(out);
    }
}
