/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimiting.tracker;

import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.threadpool.ThreadPool;

/**
 * This tracks the performance of node resources such as CPU, IO and memory
 */
public class NodePerformanceTracker extends AbstractLifecycleComponent {
    private ThreadPool threadPool;
    private final ClusterSettings clusterSettings;
    private AverageCpuUsageTracker cpuUsageTracker;
    private AverageMemoryUsageTracker memoryUsageTracker;
    private AverageDiskStats averageDiskStats;
    private AverageIOUsageTracker ioUsageTracker;
    private final FsService fsService;
    private PerformanceTrackerSettings performanceTrackerSettings;

    public NodePerformanceTracker(ThreadPool threadPool, Settings settings, ClusterSettings clusterSettings,
                                  FsService fsService) {
        this.threadPool = threadPool;
        this.clusterSettings = clusterSettings;
        this.performanceTrackerSettings = new PerformanceTrackerSettings(settings);
        this.fsService = fsService;
        initialize();
    }

    /**
     * Return CPU utilization average if we have enough datapoints, otherwise return 0
     */
    public double getCpuUtilizationPercent() {
        if (cpuUsageTracker.isReady()) {
            return cpuUsageTracker.getAverage();
        }
        return 0.0;
    }

    /**
     * Return memory utilization average if we have enough datapoints, otherwise return 0
     */
    public double getMemoryUtilizationPercent() {
        if (memoryUsageTracker.isReady()) {
            return memoryUsageTracker.getAverage();
        }
        return 0.0;
    }

    /**
     * Return if any of the resource usage trackers are ready
     */
    public boolean isReady() {
        return memoryUsageTracker.isReady() || cpuUsageTracker.isReady();
    }

    void initialize() {
        cpuUsageTracker = new AverageCpuUsageTracker(
            threadPool,
            performanceTrackerSettings.getCpuPollingInterval(),
            performanceTrackerSettings.getCpuWindowDuration()
        );
        clusterSettings.addSettingsUpdateConsumer(
            PerformanceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING,
            this::setCpuWindowDuration
        );

        memoryUsageTracker = new AverageMemoryUsageTracker(
            threadPool,
            performanceTrackerSettings.getMemoryPollingInterval(),
            performanceTrackerSettings.getMemoryWindowDuration()
        );
        clusterSettings.addSettingsUpdateConsumer(
            PerformanceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING,
            this::setMemoryWindowDuration
        );
        ioUsageTracker = new AverageIOUsageTracker(
            threadPool,
            performanceTrackerSettings.getIoPollingInterval(),
            performanceTrackerSettings.getIoWindowDuration(),
            clusterSettings,
            fsService
        );
        clusterSettings.addSettingsUpdateConsumer(
            PerformanceTrackerSettings.GLOBAL_IO_WINDOW_DURATION_SETTING,
            this::setIoWindowDuration
        );
    }

    private AverageDiskStats getAverageIOUsed() {
        return ioUsageTracker.getAverageDiskStats();
    }

    private void setAverageDiskStats(AverageDiskStats averageDiskStats) {
        this.averageDiskStats = averageDiskStats;
    }

    public AverageDiskStats getAverageDiskStats() {
        return ioUsageTracker.getAverageDiskStats();
    }

    private void setMemoryWindowDuration(TimeValue windowDuration) {
        memoryUsageTracker.setWindowSize(windowDuration);
        performanceTrackerSettings.setMemoryWindowDuration(windowDuration);
    }

    private void setCpuWindowDuration(TimeValue windowDuration) {
        cpuUsageTracker.setWindowSize(windowDuration);
        performanceTrackerSettings.setCpuWindowDuration(windowDuration);
    }

    private void setIoWindowDuration(TimeValue windowDuration) {
        ioUsageTracker.setWindowDuration(windowDuration);
        performanceTrackerSettings.setIOWindowDuration(windowDuration);
    }

    /**
     * Visible for testing
     */
    public PerformanceTrackerSettings getPerformanceTrackerSettings() {
        return performanceTrackerSettings;
    }

    @Override
    protected void doStart() {
        cpuUsageTracker.doStart();
        memoryUsageTracker.doStart();
        ioUsageTracker.doStart();
    }

    @Override
    protected void doStop() {
        cpuUsageTracker.doStop();
        memoryUsageTracker.doStop();
        ioUsageTracker.doStop();
    }

    @Override
    protected void doClose() {
        cpuUsageTracker.doClose();
        memoryUsageTracker.doClose();
        ioUsageTracker.doClose();
    }
}
