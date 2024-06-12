/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;


public abstract class CompositeMappedFieldType extends MappedFieldType {
    private final List<String> fields;
    public CompositeMappedFieldType(String name, boolean isIndexed, boolean isStored, boolean hasDocValues,
        TextSearchInfo textSearchInfo, Map<String, String> meta, List<String> fields) {
        super(name, isIndexed, isStored, hasDocValues, textSearchInfo, meta);
        this.fields = fields;
    }

    public CompositeMappedFieldType(String name, List<String> fields) {
        this(name, false, false, false, TextSearchInfo.NONE, Collections.emptyMap(), fields);
    }
}
