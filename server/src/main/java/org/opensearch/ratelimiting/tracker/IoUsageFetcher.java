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
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.fs.FsService;

import java.util.HashMap;
import java.util.Map;

/**
 * This class calculates the delta of IO utilization data such as IOPS, throughput, IO use percent etc.
 * from FS stats across all data disks
 */
class IoUsageFetcher {
    private static final Logger logger = LogManager.getLogger(IoUsageFetcher.class);
    private FsService fsService;
    public IoUsageFetcher(FsService fsService){
        this.fsService = fsService;
    }

    class DiskStats {
        public long ioTime;
        public double readTime;
        public double writeTime;
        public double readOps;
        public double writeOps;
        public long readThroughputInKB;
        public long writeThroughputInKB;
        public DiskStats(long ioTime, double readTime, double writeTime, double readOps, double writeOps,
                         long readThroughputInKB, long writeThroughputInKB) {
            this.ioTime = ioTime;
            this.readTime = readTime;
            this.writeTime = writeTime;
            this.readOps = readOps;
            this.writeOps = writeOps;
            this.readThroughputInKB = readThroughputInKB;
            this.writeThroughputInKB = writeThroughputInKB;
        }

        public long getIoTime() {
            return ioTime;
        }

        public double getReadOps() {
            return readOps;
        }

        public double getReadTime() {
            return readTime;
        }

        public long getReadThroughputInKB() {
            return readThroughputInKB;
        }

        public double getWriteOps() {
            return writeOps;
        }

        public double getWriteTime() {
            return writeTime;
        }

        public long getWriteThroughputInKB() {
            return writeThroughputInKB;
        }
    }
    public DiskStats getDiskUtilizationStats(Map<String, DiskStats> previousIOTimeMap) {
        Map<String, DiskStats> currentIOTimeMap = new HashMap<>();
        long ioUsePercent = 0;
        long readkb = 0;
        long writekb = 0;
        double readTime = 0;
        double writeTime = 0;
        double readOps = 0.0;
        double writeOps = 0.0;
        // For non linux machines, this will be null
        if(this.fsService.stats().getIoStats() == null) {
            return null;
        }
        // Sum the stats across all data volumes
        for (FsInfo.DeviceStats devicesStat : this.fsService.stats().getIoStats().getDevicesStats()) {
            if (previousIOTimeMap != null && previousIOTimeMap.containsKey(devicesStat.getDeviceName())){
                long ioSpentTime = devicesStat.getCurrentIOTime() - previousIOTimeMap.get(devicesStat.getDeviceName()).ioTime;
                ioUsePercent = ioSpentTime / 10;
                readOps += devicesStat.currentReadOperations() - previousIOTimeMap.get(devicesStat.getDeviceName()).readOps;
                writeOps += devicesStat.currentWriteOperations() - previousIOTimeMap.get(devicesStat.getDeviceName()).writeOps;
                readkb += devicesStat.getCurrentReadKilobytes() - previousIOTimeMap.get(devicesStat.getDeviceName()).readThroughputInKB;
                writekb += devicesStat.getCurrentWriteKilobytes() - previousIOTimeMap.get(devicesStat.getDeviceName()).writeThroughputInKB;
                readTime += devicesStat.getCurrentReadTime() - previousIOTimeMap.get(devicesStat.getDeviceName()).readTime;
                writeTime += devicesStat.getCurrentWriteTime() - previousIOTimeMap.get(devicesStat.getDeviceName()).writeTime;
                // Avoid dividing by fractions which will give false positives in results
                if(readTime < 1) readTime = 1;
                if(readOps < 1) readOps = 1;
                if(writeOps < 1) writeOps = 1;
                if(writeTime < 1) writeTime = 1;
            }
            DiskStats ps = new DiskStats(devicesStat.getCurrentIOTime(), devicesStat.getCurrentReadTime(),
                devicesStat.getCurrentWriteTime(), devicesStat.currentReadOperations(), devicesStat.currentWriteOperations(),
                devicesStat.getCurrentReadKilobytes(), devicesStat.getCurrentWriteKilobytes());
            currentIOTimeMap.put(devicesStat.getDeviceName(), ps);
        }
        logger.debug("IO use percent : {}", ioUsePercent);
        previousIOTimeMap.putAll(currentIOTimeMap);

        return new DiskStats(ioUsePercent, readTime, writeTime, readOps, writeOps, readkb, writekb);
    }
}
