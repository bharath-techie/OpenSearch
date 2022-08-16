/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionType;

/**
 * Doc
 */
public class PitGetAllPitsAction extends ActionType<GetAllPitNodesResponse> {
    public static final PitGetAllPitsAction INSTANCE = new PitGetAllPitsAction();
    public static final String NAME = "cluster:admin/pit/readpit";

    private PitGetAllPitsAction() {
        super(NAME, GetAllPitNodesResponse::new);
    }
}
