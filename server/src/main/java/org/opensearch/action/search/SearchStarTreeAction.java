/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionType;


public class SearchStarTreeAction extends ActionType<SearchStarTreeResponse>  {
    public static final SearchStarTreeAction INSTANCE = new SearchStarTreeAction();
    public static final String NAME = "indices:data/read/search/startree";

    private SearchStarTreeAction() {
        super(NAME, SearchStarTreeResponse::new);
    }
}
