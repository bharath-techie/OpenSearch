/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

/**
 * MovingAverage is used to calculate the moving average of last 'n' observations of double type.
 *
 * @opensearch.internal
 */
public class DoubleMovingAverage {
    private final int windowSize;
    private final double[] observations;

    private volatile long count = 0;
    private volatile double sum = 0.0;
    private volatile double average = 0.0;

    public DoubleMovingAverage(int windowSize) {
        checkWindowSize(windowSize);
        this.windowSize = windowSize;
        this.observations = new double[windowSize];
    }

    /**
     * Used for changing the window size of {@code MovingAverage}.
     *
     * @param newWindowSize new window size.
     * @return copy of original object with updated size.
     */
    public DoubleMovingAverage copyWithSize(int newWindowSize) {
        DoubleMovingAverage copy = new DoubleMovingAverage(newWindowSize);
        // Start is inclusive, but end is exclusive
        long start, end = count;
        if (isReady() == false) {
            start = 0;
        } else {
            start = end - windowSize;
        }
        // If the newWindow Size is smaller than the elements eligible to be copied over, then we adjust the start value
        if (end - start > newWindowSize) {
            start = end - newWindowSize;
        }
        for (int i = (int) start; i < end; i++) {
            copy.record(observations[i % observations.length]);
        }
        return copy;
    }

    private void checkWindowSize(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("window size must be greater than zero");
        }
    }

    /**
     * Records a new observation and evicts the n-th last observation.
     */
    public synchronized double record(double value) {
        double delta = value - observations[(int) (count % observations.length)];
        observations[(int) (count % observations.length)] = value;

        count++;
        sum += delta;
        average = sum / (double) Math.min(count, observations.length);
        return average;
    }

    public double getAverage() {
        return average;
    }

    public long getCount() {
        return count;
    }

    public boolean isReady() {
        return count >= windowSize;
    }
}
