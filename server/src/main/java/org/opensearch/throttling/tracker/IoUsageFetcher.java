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
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.fs.FsService;

import java.util.HashMap;
import java.util.Map;

public class IoUsageFetcher {
    private static final Logger logger = LogManager.getLogger(AverageCpuUsageTracker.class);
    private Map<String, DiskStats> previousIOTimeMap;
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
        public long readkb;
        public long writekb;
        public DiskStats(long ioTime, double readTime, double writeTime, double readOps, double writeOps, long readkb, long writekb) {
            this.ioTime = ioTime;
            this.readTime = readTime;
            this.writeTime = writeTime;
            this.readOps = readOps;
            this.writeOps = writeOps;
            this.readkb = readkb;
            this.writekb = writekb;
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

        public long getReadkb() {
            return readkb;
        }

        public double getWriteOps() {
            return writeOps;
        }

        public double getWriteTime() {
            return writeTime;
        }

        public long getWritekb() {
            return writekb;
        }
    }
    public DiskStats getDiskUtilizationStats() {
        Map<String, DiskStats> currentIOTimeMap = new HashMap<>();
        long ioUsePercent = 0;
        long readkb = 0;
        long writekb = 0;
        double readTime = 0;
        double writeTime = 0;
        double readLatency = 0.0;
        double writeLatency = 0.0;
        double readOps = 0.0;
        double writeOps = 0.0;
        for (FsInfo.DeviceStats devicesStat : this.fsService.stats().getIoStats().getDevicesStats()) {
            if (previousIOTimeMap != null && previousIOTimeMap.containsKey(devicesStat.getDeviceName())){
                logger.info(this.fsService.stats().getTimestamp());
                long ioSpentTime = devicesStat.getCurrentIOTime() - previousIOTimeMap.get(devicesStat.getDeviceName()).ioTime;
                ioUsePercent = (ioSpentTime * 100) / (1000);
                readOps += devicesStat.currentReadOperations() - previousIOTimeMap.get(devicesStat.getDeviceName()).readOps;
                writeOps += devicesStat.currentWriteOpetations() - previousIOTimeMap.get(devicesStat.getDeviceName()).writeOps;
                readkb += devicesStat.getCurrentReadKilobytes() - previousIOTimeMap.get(devicesStat.getDeviceName()).readkb;
                writekb += devicesStat.getCurrentWriteKilobytes() - previousIOTimeMap.get(devicesStat.getDeviceName()).writekb;
                readTime += devicesStat.getCurrentReadTime() - previousIOTimeMap.get(devicesStat.getDeviceName()).readTime;
                writeTime += devicesStat.getCurrentWriteTime() - previousIOTimeMap.get(devicesStat.getDeviceName()).writeTime;
                if(readTime < 1) readTime = 1;
                if(readOps < 1) readOps = 1;
                if(writeOps < 1) writeOps = 1;
                if(writeTime < 1) writeTime = 1;
                readLatency += (readTime / readOps);
                writeLatency += (writeTime / writeOps);
            }
            DiskStats ps = new DiskStats(devicesStat.getCurrentIOTime(), devicesStat.getCurrentReadTime(),
                devicesStat.getCurrentWriteTime(), devicesStat.currentReadOperations(), devicesStat.currentWriteOpetations(),
                devicesStat.getCurrentReadKilobytes(), devicesStat.getCurrentWriteKilobytes());
            currentIOTimeMap.put(devicesStat.getDeviceName(), ps);
        }
        logger.info("Read in MB : {} , Write in MB : {}", readkb/1000, writekb/1000);
//        readLatency += (readOps / readTime) * 100;
//        writeLatency += (writeOps / writeTime) * 100;
        logger.info("read ops : {} , writeops : {} , readtime: {} , writetime: {}", readOps, writeOps, readTime, writeTime);
        logger.info("Read latency : {}  write latency : {}" , readLatency, writeLatency);
        logger.info("IO use percent : {}", ioUsePercent);
        previousIOTimeMap = currentIOTimeMap;

        return new DiskStats(ioUsePercent, readTime, writeTime, readOps, writeOps, readkb, writekb);
    }
}
