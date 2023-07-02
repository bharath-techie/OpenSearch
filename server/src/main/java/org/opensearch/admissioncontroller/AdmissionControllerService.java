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
import org.opensearch.common.ExponentiallyWeightedMovingAverage;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.process.ProcessProbe;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service
 */
public class AdmissionControllerService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(AdmissionControllerService.class);
    private final ThreadPool threadPool;
    private final TimeValue refreshInterval;
    private volatile Scheduler.Cancellable scheduledFuture;
    private Map<String, Long> previousIOTimeMap;
    private final Map<String, Queue<Double>> deviceIOUsage;

    private static final double IO_MAX_USAGE = 30;

    private static final double IO_THRESHOLD_WINDOW = 4;
    private final AtomicInteger ioLimitBreachedCount;

    private final FsService fsService;
    double EWMA_ALPHA = 0.3;
    private final ExponentiallyWeightedMovingAverage cpuExecutionEWMA;
    private final ExponentiallyWeightedMovingAverage memoryExecutionEWMA;
    private final ExponentiallyWeightedMovingAverage ioExecutionEWMA;


    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "admissioncontroller.io.monitor.refresh_interval",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    public AdmissionControllerService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, NodeEnvironment nodeEnv, FsService fsService){
        this.threadPool = threadPool;
        this.fsService = fsService;
        this.refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        this.previousIOTimeMap = new HashMap<>();
        this.deviceIOUsage = new HashMap<>();
        this.ioLimitBreachedCount = new AtomicInteger(0);
        this.cpuExecutionEWMA = new ExponentiallyWeightedMovingAverage(EWMA_ALPHA, 0);
        this.memoryExecutionEWMA = new ExponentiallyWeightedMovingAverage(EWMA_ALPHA, 0);
        this.ioExecutionEWMA = new ExponentiallyWeightedMovingAverage(EWMA_ALPHA, 0);
    }
    @Override
    protected void doStart() {
        this.scheduledFuture = threadPool.scheduleWithFixedDelay(new IOMonitor(this.fsService), refreshInterval, ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        this.scheduledFuture.cancel();
    }

    public boolean isIOInStress(){
        return this.ioLimitBreachedCount.get() >= IO_THRESHOLD_WINDOW;
    }

    public double getCPUEWMA() {
        return cpuExecutionEWMA.getAverage();
    }

    public double getMemoryEWMA() {
        return memoryExecutionEWMA.getAverage();
    }

    public double getIoEWMA() { return ioExecutionEWMA.getAverage(); }

    @Override
    protected void doClose() throws IOException {

    }

    class IOMonitor implements Runnable {

        private final FsService fsService;

        IOMonitor(FsService fsService) {
            this.fsService = fsService;
        }

        @Override
        public void run() {
            try{
                monitorIOUtilisation();
                monitorCpuUtilisation();
                monitorMemoryUtilisation();
            }catch (Exception e){
                logger.error("Exception on the getting IO utilisation");
            }
        }

        private void monitorCpuUtilisation() {
            cpuExecutionEWMA.addValue( ProcessProbe.getInstance().getProcessCpuPercent() / 100.0);
        }
        private void monitorMemoryUtilisation() {
            memoryExecutionEWMA.addValue(JvmStats.jvmStats().getMem().getHeapUsedPercent() / 100.0);
        }
        private void monitorIOUtilisation() {
            logger.info("IO stats is triggered");
            Map<String, Long> currentIOTimeMap = new HashMap<>();
            for (FsInfo.DeviceStats devicesStat : this.fsService.stats().getIoStats().getDevicesStats()) {
                logger.info("Device Id: " + devicesStat.getDeviceName() + "; IO time: " + devicesStat.getCurrentIOTime());
                logger.info("Read Latency : " + devicesStat.getCurrentReadLatency() + " Write latency : " + devicesStat.getCurrentWriteLatency());
                logger.info("Write time : " + devicesStat.getCurrentWriteTime() + "Read time : " + devicesStat.getCurrentReadTime());
                logger.info("Read latency diff : " + devicesStat.getReadLatency() + "Write latency diff" + devicesStat.getWriteLatency());
                logger.info("Read time diff : " + devicesStat.getReadTime() + "Write time diff" + devicesStat.getWriteTime());

                if (previousIOTimeMap.containsKey(devicesStat.getDeviceName())){
                    long ioSpentTime = devicesStat.getCurrentIOTime() - previousIOTimeMap.get(devicesStat.getDeviceName());
                    double ioUsePercent = (double) (ioSpentTime * 100) / (10 * 1000);
                    ioExecutionEWMA.addValue(ioUsePercent / 100.0);
                    Queue<Double> ioUsageQueue;
                    if (deviceIOUsage.containsKey(devicesStat.getDeviceName())) {
                        ioUsageQueue = deviceIOUsage.get(devicesStat.getDeviceName());
                        if (ioUsageQueue.size() == 10){
                            double oldIOUsePercent =  ioUsageQueue.remove();
                            if (oldIOUsePercent > IO_MAX_USAGE){
                                ioLimitBreachedCount.decrementAndGet();
                            }
                        }
                    }else {
                        ioUsageQueue = new LinkedList<>();
                    }
                    ioUsageQueue.add(ioUsePercent);
                    logger.info("Queue Details: " + ioUsageQueue);
                    if (ioUsePercent > IO_MAX_USAGE){
                        ioLimitBreachedCount.incrementAndGet();
                    }
                    deviceIOUsage.put(devicesStat.getDeviceName(), ioUsageQueue);
                }
                currentIOTimeMap.put(devicesStat.getDeviceName(), devicesStat.getCurrentIOTime());
            }
            previousIOTimeMap = currentIOTimeMap;
        }
    }
}

