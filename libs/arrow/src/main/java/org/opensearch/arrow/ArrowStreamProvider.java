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

@FunctionalInterface
public interface ArrowStreamProvider {
    Task create(BufferAllocator allocator);
    interface Task {
        VectorSchemaRoot init(BufferAllocator allocator);
        void run(VectorSchemaRoot root, FlushSignal flushSignal);
    }

    @FunctionalInterface
    interface FlushSignal {
        void awaitConsumption();
    }
}
