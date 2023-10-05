/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimiting.tracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This tracks the performance of node resources such as CPU, IO and memory
 */
public class NodePerformanceTracker extends AbstractLifecycleComponent {
    private ThreadPool threadPool;
    private final ClusterSettings clusterSettings;
    private AverageIOUsageTracker ioUsageTracker;

    private AverageDiskStats averageDiskStats;
    private PerformanceTrackerSettings performanceTrackerSettings;
    private FsService fsService;
    private volatile Scheduler.Cancellable scheduledFuture;
    private static final Logger logger = LogManager.getLogger(NodePerformanceTracker.class);

    public NodePerformanceTracker(ThreadPool threadPool, Settings settings, ClusterSettings clusterSettings, FsService fsService) {
        this.threadPool = threadPool;
        this.clusterSettings = clusterSettings;
        this.performanceTrackerSettings = new PerformanceTrackerSettings(settings, clusterSettings);
        this.fsService = fsService;
        initialize();
    }

    void initialize() {
        ioUsageTracker = new AverageIOUsageTracker(
            threadPool,
            new TimeValue(1, TimeUnit.SECONDS),
            new TimeValue(60, TimeUnit.SECONDS),
            clusterSettings,
            fsService
        );
    }

    private AverageDiskStats getAverageIOUsed() {
        return ioUsageTracker.getAverageDiskStats();
    }

    private void setAverageDiskStats(AverageDiskStats averageDiskStats) {
        this.averageDiskStats = averageDiskStats;
    }

    private AverageDiskStats getAverageDiskStats() {
        return averageDiskStats;
    }

    private void doRun() {
        setAverageDiskStats(getAverageIOUsed());
        logger.info(getAverageDiskStats().toString());
    }
    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            try {
                doRun();
            } catch (Exception e) {

            }
        }, new TimeValue(1, TimeUnit.SECONDS), ThreadPool.Names.GENERIC);
        ioUsageTracker.doStart();
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
        ioUsageTracker.doStop();
    }

    @Override
    protected void doClose() throws IOException {
        ioUsageTracker.doClose();
    }
}
