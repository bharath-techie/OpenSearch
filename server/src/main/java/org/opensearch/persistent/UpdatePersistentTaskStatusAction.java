/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.persistent;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.master.ClusterManagerNodeOperationRequestBuilder;
import org.opensearch.action.support.master.ClusterManagerNodeRequest;
import org.opensearch.action.support.master.TransportClusterManagerNodeAction;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport action for updating persistent tasks
 *
 * @opensearch.internal
 */
public class UpdatePersistentTaskStatusAction extends ActionType<PersistentTaskResponse> {

    public static final UpdatePersistentTaskStatusAction INSTANCE = new UpdatePersistentTaskStatusAction();
    public static final String NAME = "cluster:admin/persistent/update_status";

    private UpdatePersistentTaskStatusAction() {
        super(NAME, PersistentTaskResponse::new);
    }

    /**
     * The action request.
     *
     * @opensearch.internal
     */
    public static class Request extends ClusterManagerNodeRequest<Request> {

        private String taskId;
        private long allocationId = -1L;
        private PersistentTaskState state;

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            taskId = in.readString();
            allocationId = in.readLong();
            state = in.readOptionalNamedWriteable(PersistentTaskState.class);
        }

        public Request(String taskId, long allocationId, PersistentTaskState state) {
            this.taskId = taskId;
            this.allocationId = allocationId;
            this.state = state;
        }

        public void setTaskId(String taskId) {
            this.taskId = taskId;
        }

        public void setAllocationId(long allocationId) {
            this.allocationId = allocationId;
        }

        public void setState(PersistentTaskState state) {
            this.state = state;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(taskId);
            out.writeLong(allocationId);
            out.writeOptionalNamedWriteable(state);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (this.taskId == null) {
                validationException = addValidationError("task id must be specified", validationException);
            }
            if (this.allocationId == -1L) {
                validationException = addValidationError("allocationId must be specified", validationException);
            }
            // We cannot really check if status has the same type as task because we don't have access
            // to the task here. We will check it when we try to update the task
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(taskId, request.taskId) && allocationId == request.allocationId && Objects.equals(state, request.state);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId, allocationId, state);
        }
    }

    /**
     * The request builder.
     *
     * @opensearch.internal
     */
    public static class RequestBuilder extends ClusterManagerNodeOperationRequestBuilder<Request, PersistentTaskResponse, RequestBuilder> {

        protected RequestBuilder(OpenSearchClient client, UpdatePersistentTaskStatusAction action) {
            super(client, action, new Request());
        }

        public final RequestBuilder setTaskId(String taskId) {
            request.setTaskId(taskId);
            return this;
        }

        public final RequestBuilder setState(PersistentTaskState state) {
            request.setState(state);
            return this;
        }
    }

    /**
     * The transport action.
     *
     * @opensearch.internal
     */

    public static class TransportAction extends TransportClusterManagerNodeAction<Request, PersistentTaskResponse> {

        private final PersistentTasksClusterService persistentTasksClusterService;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            PersistentTasksClusterService persistentTasksClusterService,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                UpdatePersistentTaskStatusAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                indexNameExpressionResolver
            );
            this.persistentTasksClusterService = persistentTasksClusterService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.MANAGEMENT;
        }

        @Override
        protected PersistentTaskResponse read(StreamInput in) throws IOException {
            return new PersistentTaskResponse(in);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            // Cluster is not affected but we look up repositories in metadata
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected final void clusterManagerOperation(
            final Request request,
            final ClusterState state,
            final ActionListener<PersistentTaskResponse> listener
        ) {
            persistentTasksClusterService.updatePersistentTaskState(
                request.taskId,
                request.allocationId,
                request.state,
                ActionListener.delegateFailure(
                    listener,
                    (delegatedListener, task) -> delegatedListener.onResponse(new PersistentTaskResponse(task))
                )
            );
        }
    }
}
