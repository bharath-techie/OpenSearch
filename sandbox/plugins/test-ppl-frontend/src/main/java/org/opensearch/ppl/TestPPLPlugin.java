/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.ppl.action.MaterializeAction;
import org.opensearch.ppl.action.RestMaterializeAction;
import org.opensearch.ppl.action.RestPPLQueryAction;
import org.opensearch.ppl.action.TestPPLTransportAction;
import org.opensearch.ppl.action.TransportMaterializeAction;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

import java.util.List;
import java.util.function.Supplier;

/**
 * Example front-end plugin using analytics-engine.
 * {@code EngineContextProvider} and {@code QueryPlanExecutor}
 * are received by {@link TestPPLTransportAction} via Guice injection.
 */
public class TestPPLPlugin extends Plugin implements ActionPlugin, ExtensiblePlugin {

    /** Enables transparent materialized-view rewrite of exact-definition PPL queries. */
    public static final Setting<Boolean> MV_REWRITE_ENABLED = Setting.boolSetting(
        "analytics.mv.rewrite.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(MV_REWRITE_ENABLED);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(UnifiedPPLExecuteAction.INSTANCE, TestPPLTransportAction.class),
            new ActionHandler<>(MaterializeAction.INSTANCE, TransportMaterializeAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestPPLQueryAction(), new RestMaterializeAction());
    }
}
