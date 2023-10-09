/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimiting.tracker;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

/**
 * Settings related to node performance trackers such as polling interval, window duration etc
 */
public class PerformanceTrackerSettings {

    private static class Defaults {
        /**
         * This is the default polling interval of usage trackers to get the resource utilization data
         */
        private static final long POLLING_INTERVAL_IN_MILLIS = 500;
        /**
         * This is the default window duration on which the average resource utilization values will be calculated
         */
        private static final long WINDOW_DURATION_IN_SECONDS = 30;
        private static final long IO_POLLING_INTERVAL = 1000;
        private static final long IO_WINDOW_DURATION = 60;
    }

    public static final Setting<TimeValue> GLOBAL_CPU_USAGE_AC_POLLING_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "node.perf_tracker.global_cpu_usage.polling_interval",
        TimeValue.timeValueMillis(Defaults.POLLING_INTERVAL_IN_MILLIS),
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING = Setting.positiveTimeSetting(
        "node.perf_tracker.global_cpu_usage.window_duration",
        TimeValue.timeValueSeconds(Defaults.WINDOW_DURATION_IN_SECONDS),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> GLOBAL_JVM_USAGE_AC_POLLING_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "node.perf_tracker.global_jvmmp.polling_interval",
        TimeValue.timeValueMillis(Defaults.POLLING_INTERVAL_IN_MILLIS),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING = Setting.positiveTimeSetting(
        "node.perf_tracker.global_jvmmp.window_duration",
        TimeValue.timeValueSeconds(Defaults.WINDOW_DURATION_IN_SECONDS),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> GLOBAL_IO_AC_POLLING_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "node.perf_tracker.global_io_usage.polling_interval",
        TimeValue.timeValueMillis(Defaults.IO_POLLING_INTERVAL),
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> GLOBAL_IO_WINDOW_DURATION_SETTING = Setting.positiveTimeSetting(
        "node.perf_tracker.global_io_usage.window_duration",
        TimeValue.timeValueSeconds(Defaults.IO_WINDOW_DURATION),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private volatile TimeValue cpuWindowDuration;
    private volatile TimeValue cpuPollingInterval;
    private volatile TimeValue memoryWindowDuration;
    private volatile TimeValue memoryPollingInterval;
    private volatile TimeValue ioWindowDuration;
    private volatile TimeValue ioPollingInterval;

    public PerformanceTrackerSettings(Settings settings) {
        this.cpuPollingInterval = GLOBAL_CPU_USAGE_AC_POLLING_INTERVAL_SETTING.get(settings);
        this.cpuWindowDuration = GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.get(settings);
        this.memoryPollingInterval = GLOBAL_JVM_USAGE_AC_POLLING_INTERVAL_SETTING.get(settings);
        this.memoryWindowDuration = GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.get(settings);
        this.ioWindowDuration = GLOBAL_IO_WINDOW_DURATION_SETTING.get(settings);
        this.ioPollingInterval = GLOBAL_IO_AC_POLLING_INTERVAL_SETTING.get(settings);
    }

    public TimeValue getCpuWindowDuration() {
        return this.cpuWindowDuration;
    }

    public TimeValue getCpuPollingInterval() {
        return cpuPollingInterval;
    }

    public TimeValue getMemoryPollingInterval() {
        return memoryPollingInterval;
    }

    public TimeValue getMemoryWindowDuration() {
        return memoryWindowDuration;
    }

    public void setCpuWindowDuration(TimeValue cpuWindowDuration) {
        this.cpuWindowDuration = cpuWindowDuration;
    }

    public void setMemoryWindowDuration(TimeValue memoryWindowDuration) {
        this.memoryWindowDuration = memoryWindowDuration;
    }

    public void setIOWindowDuration(TimeValue ioWindowDuration) {
        this.ioWindowDuration = ioWindowDuration;
    }

    public TimeValue getIoWindowDuration() { return ioWindowDuration; }

    public void setIoPollingInterval(TimeValue ioPollingInterval) {
        this.ioPollingInterval = ioPollingInterval;
    }

    public TimeValue getIoPollingInterval() {
        return ioPollingInterval;
    }
}
