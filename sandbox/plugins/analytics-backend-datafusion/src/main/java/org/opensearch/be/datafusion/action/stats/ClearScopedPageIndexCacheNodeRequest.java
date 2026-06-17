/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Per-node request in the clear-scoped-cache fan-out. Carries no payload.
 *
 * @opensearch.internal
 */
public class ClearScopedPageIndexCacheNodeRequest extends TransportRequest {

    public ClearScopedPageIndexCacheNodeRequest() {}

    public ClearScopedPageIndexCacheNodeRequest(StreamInput in) throws IOException {
        super(in);
    }
}
