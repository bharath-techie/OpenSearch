/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

/**
 *  Different resource stats are defined.
 *
 *  @opensearch.internal
 */
public enum ResourceStats {
    CPU("cpu_time_in_nanos"),
    MEMORY("memory_in_bytes"),
    IoThroughput("io_throughput_mibs"),
    IoIops("io_iops");

    private final String statsName;

    ResourceStats(String statsName) {
        this.statsName = statsName;
    }

    @Override
    public String toString() {
        return statsName;
    }
}
