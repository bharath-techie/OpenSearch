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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public class AverageIOUsageTracker extends AbstractAverageUsageTracker {

    private static final Logger logger = LogManager.getLogger(AverageCpuUsageTracker.class);

    private Map<String, DevicePreviousStats> previousIOTimeMap;

    private FsService fsService;

    public AverageIOUsageTracker(
        ThreadPool threadPool,
        TimeValue pollingInterval,
        TimeValue windowDuration,
        ClusterSettings clusterSettings,
        FsService fsService
    ) {
        super(threadPool, pollingInterval, windowDuration);
        setFsService(fsService);
        clusterSettings.addSettingsUpdateConsumer(
            PerformanceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING,
            this::setWindowDuration
        );
    }

    public FsService getFsService() {
        return fsService;
    }

    public void setFsService(FsService fsService) {
        this.fsService = fsService;
    }

    class DevicePreviousStats {
        public long ioTime;
        public double readTime;
        public double writeTime;
        public double readOps;
        public double writeOps;
        public DevicePreviousStats(long ioTime, double readTime, double writeTime, double readOps, double writeOps) {
            this.ioTime = ioTime;
            this.readTime = readTime;
            this.writeTime = writeTime;
            this.readOps = readOps;
            this.writeOps = writeOps;
        }
    }

    private long monitorIOUtilisation() {
        logger.info("IO stats is triggered");
        Map<String, DevicePreviousStats> currentIOTimeMap = new HashMap<>();
        for (FsInfo.DeviceStats devicesStat : this.fsService.stats().getIoStats().getDevicesStats()) {
            logger.info("Device Id: {} , IO time : {}", devicesStat.getDeviceName(), devicesStat.getCurrentIOTime());
            logger.info("Read Latency : {} , Write latency : {} ", devicesStat.getCurrentReadLatency(), devicesStat.getCurrentWriteLatency());
            logger.info("Write time : {} , Read time : {}", devicesStat.getCurrentWriteTime(), devicesStat.getCurrentReadTime());
            logger.info("Read latency diff : {} , Write latency diff : {}", devicesStat.getReadLatency(), devicesStat.getWriteLatency());
            logger.info("Read time diff : {}, Write time diff : {}", devicesStat.getReadTime(), devicesStat.getWriteTime());

            logger.info("Read latency : " + devicesStat.getNewReadLatency() + " Write latency : " + devicesStat.getNewWriteLatency());


            if (previousIOTimeMap.containsKey(devicesStat.getDeviceName())){
                long ioSpentTime = devicesStat.getCurrentIOTime() - previousIOTimeMap.get(devicesStat.getDeviceName()).ioTime;
                double ioUsePercent = (double) (ioSpentTime * 100) / (10 * 1000);
                //ioExecutionEWMA.addValue(ioUsePercent / 100.0);

                double readOps = devicesStat.currentReadOperations() - previousIOTimeMap.get(devicesStat.getDeviceName()).readOps;
                double writeOps = devicesStat.currentWriteOpetations() - previousIOTimeMap.get(devicesStat.getDeviceName()).writeOps;

                double readTime = devicesStat.getCurrentReadTime() - previousIOTimeMap.get(devicesStat.getDeviceName()).readTime;
                double writeTime = devicesStat.getWriteTime() - previousIOTimeMap.get(devicesStat.getDeviceName()).writeTime;

                double readLatency = readOps / readTime;
                double wrieLatency = writeOps / writeTime;

                logger.info("read ops : {} , writeops : {} , readtime: {} , writetime: {}", readOps, writeOps, readTime, writeTime);
                logger.info("Read latency final : " + readLatency  + "write latency final : " + wrieLatency);

            }

            DevicePreviousStats ps = new DevicePreviousStats(devicesStat.getCurrentIOTime(), devicesStat.getCurrentReadTime(),
                devicesStat.getCurrentWriteTime(), devicesStat.currentReadOperations(), devicesStat.currentWriteOpetations());

            currentIOTimeMap.put(devicesStat.getDeviceName(), ps);
            return devicesStat.readOperations() + devicesStat.writeOperations();
        }
        previousIOTimeMap = currentIOTimeMap;
        return 0;

    }

    @Override
    public long getUsage() {
        return monitorIOUtilisation();
    }
}
