/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.tracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.DoubleMovingAverage;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class calculates the average of various IO stats such as IOPS, throughput and IO use percent
 * for a given window and polling interval
 */
public class AverageIOUsageTracker extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(AverageCpuUsageTracker.class);

    private FsService fsService;

    private final ThreadPool threadPool;

    private IoUsageFetcher ioUsageFetcher;
    private final TimeValue windowDuration;
    private final TimeValue pollingInterval;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final AtomicReference<MovingAverage> ioTimeObservations = new AtomicReference<>();
    private final AtomicReference<MovingAverage> readIopsObservations = new AtomicReference<>();
    private final AtomicReference<MovingAverage> writeIopsObservations = new AtomicReference<>();
    private final AtomicReference<MovingAverage> readKbObservations = new AtomicReference<>();
    private final AtomicReference<MovingAverage> writeKbObservations = new AtomicReference<>();
    private final AtomicReference<DoubleMovingAverage> readLatencyObservations = new AtomicReference<>();
    private final AtomicReference<DoubleMovingAverage> writeLatencyObservations = new AtomicReference<>();
    private final Map<String, IoUsageFetcher.DiskStats> previousIOTimeMap = new HashMap<>();
    public AverageIOUsageTracker(
        ThreadPool threadPool,
        TimeValue pollingInterval,
        TimeValue windowDuration,
        ClusterSettings clusterSettings,
        FsService fsService
    ) {
        setFsService(fsService);
        this.threadPool = threadPool;
        this.pollingInterval = pollingInterval;
        this.windowDuration = windowDuration;
        this.setWindowDuration(windowDuration);
        this.ioUsageFetcher = new IoUsageFetcher(fsService);
        clusterSettings.addSettingsUpdateConsumer(
            PerformanceTrackerSettings.GLOBAL_IO_WINDOW_DURATION_SETTING,
            this::setWindowDuration
        );
    }



    public FsService getFsService() {
        return fsService;
    }

    public void setFsService(FsService fsService) {
        this.fsService = fsService;
    }

    public double getIoPercentAverage() {
        return ioTimeObservations.get().getAverage();
    }

    public double getReadIopsAverage() {
        return readIopsObservations.get().getAverage();
    }

    public double getWriteIopsAverage() {
        return writeIopsObservations.get().getAverage();
    }

    public double getReadKbAverage() {
        return readKbObservations.get().getAverage();
    }

    public double getWriteKbAverage() {
        return writeKbObservations.get().getAverage();
    }

    public double getReadLatencyAverage() {
        return readLatencyObservations.get().getAverage();
    }

    public double getWriteLatencyAverage() {
        return writeLatencyObservations.get().getAverage();
    }

    public AverageDiskStats getAverageDiskStats() {
        return new AverageDiskStats(getReadIopsAverage(), getWriteIopsAverage(), getReadKbAverage(), getWriteKbAverage(),
            getReadLatencyAverage(), getWriteLatencyAverage(), getIoPercentAverage());
    }

    public void setWindowDuration(TimeValue windowDuration) {
        int windowSize = (int) (windowDuration.nanos() / pollingInterval.nanos());
        logger.debug("updated window size: {}", windowSize);
        ioTimeObservations.set(new MovingAverage(windowSize));
        readIopsObservations.set(new MovingAverage(windowSize));
        writeIopsObservations.set(new MovingAverage(windowSize));
        readKbObservations.set(new MovingAverage(windowSize));
        writeKbObservations.set(new MovingAverage(windowSize));
        readLatencyObservations.set(new DoubleMovingAverage(windowSize));
        writeLatencyObservations.set(new DoubleMovingAverage(windowSize));
    }

    private void recordUsage(IoUsageFetcher.DiskStats usage) {
        ioTimeObservations.get().record(usage.getIoTime());
        readIopsObservations.get().record((long)usage.getReadOps());
        readKbObservations.get().record(usage.getReadThroughputInKB());
        double readOps = usage.getReadOps() < 1 ? 1.0 : usage.getReadOps() * 1.0;
        double writeOps = usage.getWriteOps() < 1 ? 1.0 : usage.getWriteOps() * 1.0;
        double readTime = usage.getReadTime() < 1 ? 0.0 : usage.getReadTime();
        double writeTime = usage.getWriteTime() < 1 ? 0.0 : usage.getWriteTime();
        double readLatency = (readTime / readOps);
        double writeLatency = (writeTime/ writeOps);
        writeLatencyObservations.get().record(writeLatency);
        readLatencyObservations.get().record(readLatency);
        writeKbObservations.get().record(usage.getWriteThroughputInKB());
        writeIopsObservations.get().record((long) usage.getWriteOps());
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
    protected void doClose() throws IOException {}

    public IoUsageFetcher.DiskStats getUsage() {
        return ioUsageFetcher.getDiskUtilizationStats(previousIOTimeMap);
    }
}
