/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.controllers;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract class for Admission Controller in OpenSearch, which aims to provide resource based request admission control.
 * It provides methods for any tracking-object that can be incremented (such as memory size),
 * and admission control can be applied if configured limit has been reached
 */
public abstract class AdmissionController {

    private final AtomicLong rejectionCount;
    private final String admissionControllerName;
    final ResourceUsageCollectorService resourceUsageCollectorService;
    public final Map<String, AtomicLong> rejectionCountMap;
    public final ClusterService clusterService;

    /**
     * @param rejectionCount          initialised rejectionCount value for AdmissionController
     * @param admissionControllerName name of the admissionController
     * @param clusterService
     */
    public AdmissionController(AtomicLong rejectionCount, String admissionControllerName, ResourceUsageCollectorService resourceUsageCollectorService, ClusterService clusterService) {
        this.rejectionCount = rejectionCount;
        this.admissionControllerName = admissionControllerName;
        this.resourceUsageCollectorService = resourceUsageCollectorService;
        this.clusterService = clusterService;
        this.rejectionCountMap = ConcurrentCollections.newConcurrentMap();
    }

    /**
     * Return the current state of the admission controller
     * @return true if admissionController is enabled for the transport layer else false
     */
    public boolean isEnabledForTransportLayer(AdmissionControlMode admissionControlMode) {
        return admissionControlMode != AdmissionControlMode.DISABLED;
    }

    /**
     *
     * @return true if admissionController is Enforced Mode else false
     */
    public Boolean isAdmissionControllerEnforced(AdmissionControlMode admissionControlMode) {
        return admissionControlMode == AdmissionControlMode.ENFORCED;
    }

    /**
     * Increment the tracking-objects and apply the admission control if threshold is breached.
     * Mostly applicable while applying admission controller
     */
    public abstract void apply(String action, AdmissionControlActionType admissionControlActionType);

    /**
     * @return name of the admission-controller
     */
    public String getName() {
        return this.admissionControllerName;
    }

    public void addRejectionCount(String admissionControlActionType, long count) {
        AtomicLong updatedCount = new AtomicLong(0);
        if(this.rejectionCountMap.containsKey(admissionControlActionType)){
            updatedCount.addAndGet(this.rejectionCountMap.get(admissionControlActionType).get());
        }
        updatedCount.addAndGet(count);
        this.rejectionCountMap.put(admissionControlActionType, updatedCount);
    }

    /**
     * @return current value of the rejection count metric tracked by the admission-controller.
     */
    public long getRejectionCount(String admissionControlActionType) {
        AtomicLong rejectionCount = this.rejectionCountMap.getOrDefault(admissionControlActionType, new AtomicLong());
        return rejectionCount.get();
    }

    public Map<String, Long> getRejectionStats() {
        Map<String, Long> rejectionStats = new HashMap<>();
        rejectionCountMap.forEach((actionType, count) -> rejectionStats.put(actionType, count.get()));
        return rejectionStats;
    }
}
