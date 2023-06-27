/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.admissioncontroller;

import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.transport.TransportInterceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Plugin
 */
public class AdmissionControllerPlugin extends Plugin implements NetworkPlugin {

    public AdmissionControllerService admissionControllerService;
    public AdmissionControllerPlugin(AdmissionControllerService admissionControllerService) {
        this.admissionControllerService = admissionControllerService;
    }
    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        List<TransportInterceptor> interceptors = new ArrayList<TransportInterceptor>(1);
        interceptors.add(new AdmissionControllerTransportInterceptor(null, this.admissionControllerService));
        return interceptors;
    }
}
