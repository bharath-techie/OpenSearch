/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Functional interface for providing Arrow streams.
 * This interface defines the contract for creating and managing Arrow stream tasks.
 */
public interface StreamProvider {

    /**
     * Initializes the task with a given buffer allocator.
     *
     * @param allocator The buffer allocator to use for initialization.
     * @return A new VectorSchemaRoot instance.
     */
    VectorSchemaRoot createRoot(BufferAllocator allocator);

    /**
     * Creates a new Arrow stream task with the given buffer allocator.
     *
     * @param allocator The buffer allocator to use for the task.
     * @return A new Task instance.
     */
    BatchedJob createJob(BufferAllocator allocator);

    default int estimatedRowCount() {
        return -1;
    }

    /**
     * Represents a task for managing an Arrow stream.
     */
    interface BatchedJob {

        /**
         * Runs the task with the given VectorSchemaRoot and FlushSignal.
         *
         * @param root The VectorSchemaRoot to use for the task.
         * @param flushSignal The FlushSignal to use for managing consumption.
         */
        void run(VectorSchemaRoot root, FlushSignal flushSignal);

        /**
         * Called when the task is canceled.
         * This method is used to clean up resources or cancel ongoing operations.
         * This maybe called from a different thread than the one used for run(). It might be possible that run()
         * thread is busy when onCancel() is called and wakes up later. In such cases, ensure that run() terminates early
         * and should clean up resources.
         */
        void onCancel();
    }

    /**
     * Functional interface for managing stream consumption signals.
     */
    @FunctionalInterface
    interface FlushSignal {
        /**
         * Waits for the consumption of the current data to complete.
         * This method blocks until the consumption is complete or a timeout occurs.
         *
         * @param timeout The maximum time to wait for consumption (in milliseconds).
         */
        void awaitConsumption(int timeout);
    }
}
