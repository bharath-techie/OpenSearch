/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves per-field storage metadata from {@link IndexMetadata}.
 *
 * <p>Uses the index's {@code index.composite.primary_data_format} setting as the
 * doc value format for all fields that have doc values. Index formats are always
 * {@code "lucene"} for fields explicitly marked {@code index: true}. Stored fields
 * are {@code "lucene"} for fields explicitly marked {@code store: true}.
 *
 * <p>TODO: Replace with actual per-field format metadata once the indexing team adds
 * {@code doc_value_formats} / {@code index_formats} to MappingMetadata.
 *
 * @opensearch.internal
 */
public class FieldStorageResolver {

    // TODO: import from CompositeEnginePlugin.PRIMARY_DATA_FORMAT once composite-common
    // exposes it as a shared constant accessible to analytics-engine.
    static final String PRIMARY_DATA_FORMAT_SETTING = "index.composite.primary_data_format";
    static final String SECONDARY_DATA_FORMATS_SETTING = "index.composite.secondary_data_formats";

    private static final String LUCENE_FORMAT = "lucene";

    private final Map<String, FieldStorageInfo> fieldStorage;

    /**
     * Test constructor — explicit per-field storage, bypasses IndexMetadata inference.
     * Allows tests to declare hybrid fields (e.g. doc values in both parquet and lucene)
     * without needing actual IndexMetadata.
     *
     * TODO: remove once FieldStorageResolver is integrated with actual per-field format
     * metadata from MappingMetadata — tests should use real mappings at that point.
     */
    FieldStorageResolver(Map<String, FieldStorageInfo> fieldStorage) {
        this.fieldStorage = new HashMap<>(fieldStorage);
    }

    @SuppressWarnings("unchecked")
    public FieldStorageResolver(IndexMetadata indexMetadata) {
        String indexName = indexMetadata.getIndex().getName();
        String primaryFormat = indexMetadata.getSettings().get(PRIMARY_DATA_FORMAT_SETTING, LUCENE_FORMAT);
        // Lucene is index-viable only when it's the primary or in the secondary list.
        boolean luceneAvailable = LUCENE_FORMAT.equals(primaryFormat)
            || indexMetadata.getSettings().getAsList(SECONDARY_DATA_FORMATS_SETTING).contains(LUCENE_FORMAT);

        // A mapping-less index (created empty, never written to) declares no fields — it
        // contributes nothing to the field-storage union. Aliases and index patterns legitimately
        // span such indices next to populated ones (schema-side resolution skips them the same
        // way), so treat "no mapping" / "no properties" as an empty field set rather than an error.
        // A query that references only such indices fails upstream at Calcite validation with
        // "table not found" (the schema builder yields no row type), never reaching this resolver.
        MappingMetadata mapping = indexMetadata.mapping();
        Map<String, Object> properties = mapping == null ? null : (Map<String, Object>) mapping.sourceAsMap().get("properties");

        this.fieldStorage = new HashMap<>();
        if (properties != null) {
            populateFromProperties(properties, "", primaryFormat, luceneAvailable);
        }
    }

    @SuppressWarnings("unchecked")
    private void populateFromProperties(Map<String, Object> properties, String pathPrefix, String primaryFormat, boolean luceneAvailable) {
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String fieldName = pathPrefix.isEmpty() ? entry.getKey() : pathPrefix + "." + entry.getKey();
            Map<String, Object> fieldProps = (Map<String, Object>) entry.getValue();
            String fieldType = (String) fieldProps.get("type");
            if (fieldType == null) {
                // Implicit "object" type — OpenSearch infers it from presence of "properties".
                // Recurse into the sub-mapping; object fields themselves have no storage.
                Map<String, Object> nested = (Map<String, Object>) fieldProps.get("properties");
                if (nested != null) {
                    populateFromProperties(nested, fieldName, primaryFormat, luceneAvailable);
                    continue;
                }
                throw new IllegalStateException("Field [" + fieldName + "] has no type in mapping");
            }
            this.fieldStorage.put(fieldName, resolveField(fieldName, fieldType, fieldProps, primaryFormat, luceneAvailable));
        }
    }

    /**
     * Unions the field storage of several per-index resolvers into one. First declaration of a
     * field wins; {@link IndexResolution#resolve} has already verified that any field declared by
     * more than one backing index agrees on type, so the choice of source index is immaterial.
     *
     * <p>Used when a table name resolves to multiple concrete indices (alias or index pattern) with
     * differing field sets: the scan's row type is the union across all of them, so resolving every
     * requested field against a single index would spuriously fail on fields that index omits.
     */
    static FieldStorageResolver merged(List<FieldStorageResolver> perIndex) {
        Map<String, FieldStorageInfo> union = new HashMap<>();
        for (FieldStorageResolver resolver : perIndex) {
            for (Map.Entry<String, FieldStorageInfo> entry : resolver.fieldStorage.entrySet()) {
                union.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
        return new FieldStorageResolver(union);
    }

    /** Resolves storage info for the requested fields in order. */
    public List<FieldStorageInfo> resolve(List<String> fieldNames) {
        List<FieldStorageInfo> result = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
            FieldStorageInfo info = fieldStorage.get(fieldName);
            if (info == null) {
                throw new IllegalArgumentException("Field [" + fieldName + "] not found in field storage for index");
            }
            result.add(info);
        }
        return result;
    }

    private static FieldStorageInfo resolveField(
        String fieldName,
        String fieldType,
        Map<String, Object> fieldProps,
        String primaryFormat,
        boolean luceneAvailable
    ) {
        // Doc values: present for all types unless explicitly disabled
        boolean hasDocValues = !Boolean.FALSE.equals(fieldProps.get("doc_values"));

        // Index: only when explicitly set to false in mapping - enabled by default.
        boolean isIndexed = !Boolean.FALSE.equals(fieldProps.get("index"));

        // Stored fields: only when explicitly set to true in mapping
        boolean isStored = Boolean.TRUE.equals(fieldProps.get("store"));

        List<String> docValueFormats = hasDocValues ? List.of(primaryFormat) : List.of();
        // Only declare Lucene formats when Lucene is actually an index data format.
        List<String> indexFormats = (isIndexed && luceneAvailable) ? List.of(LUCENE_FORMAT) : List.of();
        List<String> storedFieldFormats = (isStored && luceneAvailable) ? List.of(LUCENE_FORMAT) : List.of();

        if (docValueFormats.isEmpty() && indexFormats.isEmpty() && storedFieldFormats.isEmpty()) {
            throw new IllegalStateException("Field [" + fieldName + "] has no storage in any format");
        }

        return new FieldStorageInfo(
            fieldName,
            fieldType,
            FieldType.fromMappingType(fieldType),
            docValueFormats,
            indexFormats,
            storedFieldFormats,
            false,
            exactMatchSubfieldOf(fieldType, fieldProps),
            isExactTermDelegatable(fieldType, fieldProps)
        );
    }

    /**
     * Decides whether a doc-value (columnar) backend's raw equality on this field is semantically
     * identical to the index (Lucene) term query — i.e. whether an exact-match (term) predicate on
     * this field may be dual-viable (performance-delegated) rather than Lucene-only correctness
     * delegation. See {@link FieldStorageInfo#isExactTermDelegatable()}.
     *
     * <p>Conservative by construction — only mappings whose stored term is a single, full-value,
     * untransformed representation are eligible:
     *
     * <ul>
     *   <li>{@code keyword} / {@code constant_keyword} WITHOUT a {@code normalizer} — the stored
     *       term is the raw value; doc-value equality matches the term exactly.</li>
     *   <li>numeric, {@code date}/{@code date_nanos}, {@code boolean}, {@code ip} — point values
     *       whose columnar equality matches the indexed term exactly.</li>
     * </ul>
     *
     * Everything else is Lucene-only:
     * <ul>
     *   <li>{@code text} / {@code match_only_text} — an analyzer tokenizes the value; only the term
     *       index reproduces the match, a columnar full-string equality does not.</li>
     *   <li>{@code keyword}/{@code constant_keyword} WITH a {@code normalizer}, {@code wildcard},
     *       and any transformed mapping — the stored/queried term is rewritten; a doc-value backend
     *       does not reproduce the transformation.</li>
     *   <li>unknown / unmapped types — conservative default of Lucene-only.</li>
     * </ul>
     */
    private static boolean isExactTermDelegatable(String mappingType, Map<String, Object> fieldProps) {
        FieldType type = FieldType.fromMappingType(mappingType);
        if (type == null) {
            // Unknown/unmapped type — DataFusion cannot be assumed to reproduce the term semantics.
            return false;
        }
        // Tokenizing text: term match needs the analyzer's token index → Lucene-only.
        // DELIBERATE MINIMAL POLICY: we keep ALL text/match_only_text Lucene-only even for the rare
        // field configured with a non-tokenizing (e.g. keyword) analyzer that could in principle be
        // dual-viable. This is correctness-safe — it only forgoes a performance optimization for an
        // uncommon mapping — and avoids threading analyzer resolution through here for now.
        if (FieldType.text().contains(type)) {
            return false;
        }
        // Keyword family: exact full-value term ONLY when no normalizer transforms the stored value.
        // WILDCARD stores an ngram-transformed representation, so it is never exact-term delegatable.
        if (type == FieldType.KEYWORD || type == FieldType.CONSTANT_KEYWORD) {
            return fieldProps.get("normalizer") == null;
        }
        if (type == FieldType.WILDCARD_FIELD) {
            return false;
        }
        // Numeric, date, boolean, ip: columnar equality is exact and matches the indexed term.
        return FieldType.numeric().contains(type)
            || FieldType.date().contains(type)
            || type == FieldType.BOOLEAN
            || type == FieldType.IP;
    }

    /**
     * For a {@code text} field with a {@code fields} multifield block, returns the name of the
     * first {@code keyword} subfield (e.g. {@code "keyword"}), or {@code null} if there is none.
     * Exact-equality predicates route to this subfield (see {@link FieldStorageInfo#getExactMatchSubfield()}).
     */
    @SuppressWarnings("unchecked")
    private static String exactMatchSubfieldOf(String fieldType, Map<String, Object> fieldProps) {
        if (!"text".equals(fieldType)) {
            return null;
        }
        Object fields = fieldProps.get("fields");
        if (!(fields instanceof Map<?, ?> subfields)) {
            return null;
        }
        for (Map.Entry<?, ?> entry : subfields.entrySet()) {
            if (entry.getValue() instanceof Map<?, ?> subProps && "keyword".equals(subProps.get("type"))) {
                return String.valueOf(entry.getKey());
            }
        }
        return null;
    }
}
