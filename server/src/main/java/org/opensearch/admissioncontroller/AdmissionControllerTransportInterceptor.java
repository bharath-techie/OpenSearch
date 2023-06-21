/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.admissioncontroller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

public class AdmissionControllerTransportInterceptor implements TransportInterceptor {

    protected final Logger log = LogManager.getLogger(this.getClass());
    protected final ThreadPool threadPool;
    public AdmissionControllerService admissionControllerService;

    public AdmissionControllerTransportInterceptor(final ThreadPool threadPool, AdmissionControllerService admissionControllerService) {
        this.threadPool = threadPool;
        this.admissionControllerService = admissionControllerService;
    }

    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, String executor, boolean forceExecution,
                                                                                    TransportRequestHandler<T> actualHandler) {
        return new AdmissionControllerRequestHandler<T>(action, actualHandler, threadPool, admissionControllerService);
    }
}
