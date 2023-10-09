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
import org.opensearch.common.ExponentiallyWeightedMovingAverage;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;

public class IOEwmaUsageTracker extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(IOEwmaUsageTracker.class);

    private FsService fsService;

    private final ThreadPool threadPool;

    private IoUsageFetcher ioUsageFetcher;
    private final TimeValue windowDuration;
    private final TimeValue pollingInterval;
    private volatile Scheduler.Cancellable scheduledFuture;


    private ExponentiallyWeightedMovingAverage ioTimeEWMA;
    private ExponentiallyWeightedMovingAverage readThroughputEWMA;
    private ExponentiallyWeightedMovingAverage readIopsEWMA;
    private ExponentiallyWeightedMovingAverage writeThroughputEWMA;
    private ExponentiallyWeightedMovingAverage writeIopsEWMA;
    private ExponentiallyWeightedMovingAverage readLatencyEWMA;
    private ExponentiallyWeightedMovingAverage writeLatencyEwma;
    private ExponentiallyWeightedMovingAverage averageQueueSizeEwma;

    private final Map<String, IoUsageFetcher.DiskStats> previousIOTimeMap = new HashMap<>();
    public IOEwmaUsageTracker(
        ThreadPool threadPool,
        TimeValue pollingInterval,
        TimeValue windowDuration,
        ClusterSettings clusterSettings,
        FsService fsService,
        ExponentiallyWeightedMovingAverage ioTimeEwma) {
        long period = 60;
        double alpha = 2 / (period + 1);
        this.ioTimeEWMA = new ExponentiallyWeightedMovingAverage(alpha, 0.0);
        setFsService(fsService);
        this.threadPool = threadPool;
        this.pollingInterval = pollingInterval;
        this.windowDuration = windowDuration;
        this.setWindowDuration(windowDuration);
        this.ioUsageFetcher = new IoUsageFetcher(fsService);

        // Add this post integration
//        clusterSettings.addSettingsUpdateConsumer(
//            PerformanceTrackerSettings.GLOBAL_IO_WINDOW_DURATION_SETTING,
//            this::setWindowDuration
//        );
    }


    public FsService getFsService() {
        return fsService;
    }

    public void setFsService(FsService fsService) {
        this.fsService = fsService;
    }

    public double getIoPercentAverage() {
        return ioTimeEWMA.getAverage();
    }

    public double getReadIopsAverage() {
        return readIopsEWMA.getAverage();
    }

    public double getWriteIopsAverage() {
        return writeIopsEWMA.getAverage();
    }

    public double getReadKbAverage() {
        return readThroughputEWMA.getAverage();
    }

    public double getWriteKbAverage() {
        return writeThroughputEWMA.getAverage();
    }

    public double getReadLatencyAverage() {
        return readLatencyEWMA.getAverage();
    }

    public double getWriteLatencyAverage() {
        return writeLatencyEwma.getAverage();
    }

    public double getQueueSizeAverage() { return averageQueueSizeEwma.getAverage(); }

    public AverageDiskStats getAverageDiskStats() {
        return new AverageDiskStats(getReadIopsAverage(), getWriteIopsAverage(), getReadKbAverage(), getWriteKbAverage(),
            getReadLatencyAverage(), getWriteLatencyAverage(), getIoPercentAverage(), getQueueSizeAverage());
    }

    public void setWindowDuration(TimeValue windowDuration) {
        int windowSize = (int) (windowDuration.nanos() / pollingInterval.nanos());
        logger.debug("updated window size: {}", windowSize);
        double alpha = 2 / (windowSize + 1);
        ioTimeEWMA = new ExponentiallyWeightedMovingAverage(alpha, 0);
        readIopsEWMA = new ExponentiallyWeightedMovingAverage(alpha, 0);
        readThroughputEWMA = new ExponentiallyWeightedMovingAverage(alpha, 0);
        readLatencyEWMA = new ExponentiallyWeightedMovingAverage(alpha, 0);
        writeIopsEWMA = new ExponentiallyWeightedMovingAverage(alpha, 0);
        writeLatencyEwma = new ExponentiallyWeightedMovingAverage(alpha, 0);
        writeThroughputEWMA = new ExponentiallyWeightedMovingAverage(alpha, 0);
        averageQueueSizeEwma = new ExponentiallyWeightedMovingAverage(alpha, 0);

    }

    private void recordUsage(IoUsageFetcher.DiskStats usage) {
        double readOps = usage.getReadOps() < 1 ? 1.0 : usage.getReadOps();
        double writeOps = usage.getWriteOps() < 1 ? 1.0 : usage.getWriteOps();
        double readTime = usage.getReadTime() < 1 ? 0.0 : usage.getReadTime();
        double writeTime = usage.getWriteTime() < 1 ? 0.0 : usage.getWriteTime();
        double readLatency = (readTime / readOps);
        double writeLatency = (writeTime/ writeOps);
        ioTimeEWMA.addValue(usage.getIoTime());
        readIopsEWMA.addValue(usage.getReadOps());
        readThroughputEWMA.addValue(usage.getReadThroughputInKB());
        readLatencyEWMA.addValue(readLatency);
        writeLatencyEwma.addValue(usage.getWriteOps());
        writeThroughputEWMA.addValue(usage.getWriteThroughputInKB());
        averageQueueSizeEwma.addValue(usage.getQueueSize());
    }

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            IoUsageFetcher.DiskStats usage = getUsage();
            if(usage == null) return;
            recordUsage(usage);
        }, pollingInterval, ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() {}

    public IoUsageFetcher.DiskStats getUsage() {
        return ioUsageFetcher.getDiskUtilizationStats(previousIOTimeMap);
    }
}
