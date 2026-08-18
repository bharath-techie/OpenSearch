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
 * Instruction node for final aggregate in coordinator reduce — ExchangeSink path,
 * remove partial agg, preserve final-only for the driving backend's reduce execution.
 *
 * <p>When {@link #emitStates()} is set (materialized-view refresh), the reduce folds
 * the shards' partial aggregate states by group key and emits the folded states
 * (columns named {@code {call_alias}__st_i}) instead of finalized values, so the
 * materialize sink writes re-mergeable state segments.
 *
 * @opensearch.internal
 */
public class FinalAggregateInstructionNode implements InstructionNode {

    private final boolean emitStates;

    public FinalAggregateInstructionNode() {
        this(false);
    }

    public FinalAggregateInstructionNode(boolean emitStates) {
        this.emitStates = emitStates;
    }

    public FinalAggregateInstructionNode(StreamInput in) throws IOException {
        this.emitStates = in.readBoolean();
    }

    /** Whether the reduce emits folded aggregate states instead of finalized values. */
    public boolean emitStates() {
        return emitStates;
    }

    @Override
    public InstructionType type() {
        return InstructionType.SETUP_FINAL_AGGREGATE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(emitStates);
    }
}
