/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.action.ActionType;

/**
 * Action singleton for materializing a PPL query result into a target index.
 *
 * <p>The name is the cross-plugin contract: orchestrating plugins (index-management
 * materialized-view refresh jobs) invoke it via {@code client.execute} with their own
 * wire-compatible request/response classes registered under this same name.
 */
public class MaterializeAction extends ActionType<MaterializeResponse> {
    public static final String NAME = "indices:admin/analytics/materialize";
    public static final MaterializeAction INSTANCE = new MaterializeAction();

    private MaterializeAction() {
        super(NAME, MaterializeResponse::new);
    }
}
