/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request to materialize a PPL query's result into a target index: the query executes on
 * the analytics engine and its Arrow result stream is bulk-written into {@code targetIndex}
 * via the streaming index sink — never buffered on the coordinator.
 *
 * <p>Wire-serializable so scheduled-job orchestrators (e.g. index-management materialized
 * view refresh jobs) can invoke the action by name with a wire-compatible request class of
 * their own, without a compile-time dependency on this plugin.
 */
public class MaterializeRequest extends ActionRequest {

    private final String pplQuery;
    private final String targetIndex;
    /** Column names whose values form the deterministic document id; empty → auto ids. */
    private final List<String> keyColumns;
    /**
     * Materialized-view refresh: the reduce folds partial aggregate states and streams
     * folded states (columns {@code {alias}__st_i}) into the target index instead of
     * finalized values; the sink provisions the view index from the plan-derived spec.
     */
    private final boolean emitStates;

    public MaterializeRequest(String pplQuery, String targetIndex, List<String> keyColumns) {
        this(pplQuery, targetIndex, keyColumns, false);
    }

    public MaterializeRequest(String pplQuery, String targetIndex, List<String> keyColumns, boolean emitStates) {
        this.pplQuery = pplQuery;
        this.targetIndex = targetIndex;
        this.keyColumns = keyColumns == null ? List.of() : List.copyOf(keyColumns);
        this.emitStates = emitStates;
    }

    public MaterializeRequest(StreamInput in) throws IOException {
        super(in);
        this.pplQuery = in.readString();
        this.targetIndex = in.readString();
        this.keyColumns = in.readStringList();
        this.emitStates = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(pplQuery);
        out.writeString(targetIndex);
        out.writeStringCollection(keyColumns);
        out.writeBoolean(emitStates);
    }

    public String getPplQuery() {
        return pplQuery;
    }

    public String getTargetIndex() {
        return targetIndex;
    }

    public List<String> getKeyColumns() {
        return keyColumns;
    }

    /** Whether the refresh streams folded aggregate states instead of finalized values. */
    public boolean isEmitStates() {
        return emitStates;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (pplQuery == null || pplQuery.isBlank()) {
            validationException = addValidationError("query must not be empty", validationException);
        }
        if (targetIndex == null || targetIndex.isBlank()) {
            validationException = addValidationError("target_index must not be empty", validationException);
        }
        return validationException;
    }
}
