/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import java.util.Map;
import org.apache.lucene.search.Query;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;


public class CompositeFieldType extends MappedFieldType {
    public CompositeFieldType(String name, boolean isIndexed, boolean isStored, boolean hasDocValues,
        TextSearchInfo textSearchInfo, Map<String, String> meta) {
        super(name, isIndexed, isStored, hasDocValues, textSearchInfo, meta);
    }

    @Override
    public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        return null;
    }

    @Override
    public String typeName() {
        return null;
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        return null;
    }
}
