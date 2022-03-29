/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionType;

public class GetAllPITsAction extends ActionType<GetAllPITNodesResponse> {


    public static final GetAllPITsAction INSTANCE = new GetAllPITsAction();
    public static final String NAME = "indices:data/readall/pit";

    private GetAllPITsAction() {
        super(NAME, GetAllPITNodesResponse::new);
    }

}
