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
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

/**
 * Handler
 * @param <T>
 */
public class AdmissionControllerRequestHandler<T extends TransportRequest> implements TransportRequestHandler<T> {
    private final String action;
    private final TransportRequestHandler<T> actualHandler;
    private final ThreadPool threadPool;
    protected final Logger log = LogManager.getLogger(this.getClass());
    public AdmissionControllerService admissionControllerService;
    public AdmissionControllerRequestHandler(String action, TransportRequestHandler<T> actualHandler,
                                             ThreadPool threadPool, AdmissionControllerService admissionControllerService) {
        super();
        this.action = action;
        this.actualHandler = actualHandler;
        this.threadPool = threadPool;
        this.admissionControllerService = admissionControllerService;
    }

    protected ThreadContext getThreadContext() {
        if(threadPool == null) {
            return null;
        }
        threadPool.getThreadContext().getTransient("PERF_STATS");
        return threadPool.getThreadContext();
    }

    public boolean isSearchRequest() {
        return this.action.startsWith("indices:data/read/search");
    }

    public boolean isIndexRequest(){
        return this.action.startsWith("indices:data/write");
    }

    @Override
    public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
        // Evaluate the requests here.
        if (this.admissionControllerService.isIOInStress()) {
            log.info("Admission controller service responded with IO is in stress state");
//            if (this.isSearchRequest()){
//                channel.sendResponse(new OpenSearchRejectedExecutionException("Execution Rejected due to high IO usage"));
//                return;
//            }
        }else {
            //log.info("Admission controller service responded with IO is in healthy state");
        }
        this.messageReceivedDecorate(request, actualHandler, channel, task);
    }

    protected void messageReceivedDecorate(final T request, final TransportRequestHandler<T> actualHandler, final TransportChannel transportChannel, Task task) throws Exception {
        actualHandler.messageReceived(request, transportChannel, task);
    }
}
