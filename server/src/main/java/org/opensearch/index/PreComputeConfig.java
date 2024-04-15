/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.opensearch.common.settings.Setting;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.sort.SortOrder;


/**

 PreComputeConfig : [ {
    "dimensions": ["status", "client_ip"],
    "metrics" : [
        {
            "field" : "request_count",
            "type" : "sum"
        }
    ],
     "timestamp" : [
        {
         "field" : "@timestamp",
         "granularity" : ["minute", "hour", "day", "month", "year"]
        }
     ],
 }]


 */

public final class PreComputeConfig {

    /**
     * The list of field names
     */
    public static final Setting<List<String>> INDEX_SORT_FIELD_SETTING = Setting.listSetting(
        "index.precompute.ordered.dimensions",
        Collections.emptyList(),
        Function.identity(),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * The {@link SortOrder} for each specified sort field (ie. <b>asc</b> or <b>desc</b>).
     */
//    public static final Setting<List<SortOrder>> INDEX_SORT_ORDER_SETTING = Setting.listSetting(
//        "index.sort.order",
//        Collections.emptyList(),
//        IndexSortConfig::parseOrderMode,
//        Setting.Property.IndexScope,
//        Setting.Property.Final
//    );

    /**
     * The {@link MultiValueMode} for each specified sort field (ie. <b>max</b> or <b>min</b>).
     */
//    public static final Setting<List<MultiValueMode>> INDEX_SORT_MODE_SETTING = Setting.listSetting(
//        "index.sort.mode",
//        Collections.emptyList(),
//        IndexSortConfig::parseMultiValueMode,
//        Setting.Property.IndexScope,
//        Setting.Property.Final
//    );

    /**
     * The missing value for each specified sort field (ie. <b>_first</b> or <b>_last</b>)
     */
//    public static final Setting<List<String>> INDEX_SORT_MISSING_SETTING = Setting.listSetting(
//        "index.sort.missing",
//        Collections.emptyList(),
//        IndexSortConfig::validateMissingValue,
//        Setting.Property.IndexScope,
//        Setting.Property.Final
//    );
}
