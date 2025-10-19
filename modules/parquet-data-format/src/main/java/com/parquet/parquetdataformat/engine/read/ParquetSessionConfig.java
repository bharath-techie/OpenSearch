/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.engine.read;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.vectorized.execution.search.spi.ConfigUpdateListener;
import org.opensearch.vectorized.execution.search.spi.SessionConfig;
import org.opensearch.vectorized.execution.search.spi.SessionConfigRegistry;

public class ParquetSessionConfig implements SessionConfig {

    public static final Setting<Integer> PARQUET_BATCH_SIZE = Setting.intSetting(
        "parquet.batch_size",
        1024,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );

    SessionConfigRegistry sessionConfigRegistry;
    Integer parquetBatchSize;

    ParquetSessionConfig(ClusterService clusterService) {
        super();
        sessionConfigRegistry = new SessionConfigRegistry();

        parquetBatchSize = PARQUET_BATCH_SIZE.get(clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(PARQUET_BATCH_SIZE, this::setBatchSize);
    }

    @Override
    public void setBatchSize(int batchSize){
        parquetBatchSize = batchSize;
//        updateParquetSessionConfig(nativeSessionConfigPtr, "batch_size", String.valueOf(batchSize));
        sessionConfigRegistry.publishSessionConfigUpdate(this);
    }

    @Override
    public void mergeFrom(SessionConfig other) {
        throw new UnsupportedOperationException("Parquet can't merge from other config");
    }

    @Override
    public Integer getBatchSize() {
        return parquetBatchSize;
//        return getParquetSessionConfigValue(nativeSessionConfigPtr, "batch_size");
    }

    public void registerListener(ConfigUpdateListener listener) {
        sessionConfigRegistry.registerListener(listener);
    }


}
