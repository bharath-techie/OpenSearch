/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Filters the Tokio runtime worker thread from thread-leak checks.
 *
 * The Tokio runtime (initialized via {@code NativeBridge.initTokioRuntimeManager})
 * spawns native background threads that live for the JVM's lifetime. These threads
 * have empty Java stacks and generic names (e.g. "Thread-5") because they are
 * native Rust threads. They are not a leak — they are shut down when the runtime
 * manager is dropped at process exit.
 */
public final class TokioThreadLeakFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
        // Tokio native worker threads have empty stack traces and generic names.
        // They are always RUNNABLE with no Java frames.
        if (t.getStackTrace().length == 0 && t.getName().startsWith("Thread-")) {
            return true;
        }
        // Named datafusion threads (IO workers, CPU executor driver)
        String name = t.getName();
        return name.startsWith("datafusion-io") || name.startsWith("datafusion-cpu");
    }
}
