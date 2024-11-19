/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.tasks.TaskId;

/**
 * Abstract class for managing Arrow streams.
 * This class provides functionality for registering, retrieving, and removing stream producers.
 * It also manages the lifecycle of streams and their associated resources.
 */
@ExperimentalApi
public interface StreamManager extends AutoCloseable {

    /**
     * Registers a new stream with the given ArrowStreamProvider.
     *
     * @param producer The StreamProducer to register.
     * @param parentTaskId The TaskId of the parent task.
     * @return A new StreamTicket for the registered stream.
     */
    StreamTicket registerStream(StreamProducer producer, TaskId parentTaskId);

    /**
     * Retrieves the StreamIterator for the given StreamTicket.
     *
     * @param ticket The StreamTicket of the desired stream.
     * @return The StreamIterator for the associated stream.
     */
    StreamIterator getStreamIterator(StreamTicket ticket);

    /**
     * Generates a unique StreamTicket.
     *
     * @return A new, unique StreamTicket.
     */
    String generateUniqueTicket();

    /**
     * Returns the ID of the local node. This is useful to embed the node id into ticket.
     * NodeId can be used to identify the node where the stream is registered.
     *
     * @return The ID of the local node.
     */
    String getLocalNodeId();
}
