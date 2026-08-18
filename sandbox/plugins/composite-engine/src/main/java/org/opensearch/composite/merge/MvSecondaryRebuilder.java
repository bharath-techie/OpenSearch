/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.exec.PrimaryTermFieldType;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.mapper.VersionFieldMapper;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rebuilds a secondary format's merged-segment content from the primary format's folded
 * (aggregated) merge output during a materialized-view merge.
 *
 * <p>An aggregating merge folds N input rows into M result rows — the folded rows are
 * <em>new</em> rows with new row IDs {@code 0..M-1}. This class re-drives each folded row
 * through the document mapper into a fresh writer of the secondary format at the merged
 * generation, so every format holds exactly the same M rows and cross-format row parity
 * is preserved by construction.
 *
 * <p>Metadata fields are taken from the folded row's own passthrough columns ({@code _id},
 * {@code _seq_no}, {@code _version}, {@code _primary_term}, {@code _routing}) — the same
 * folded values the primary format stores, so the formats agree cell-for-cell.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MvSecondaryRebuilder {

    private static final Logger logger = LogManager.getLogger(MvSecondaryRebuilder.class);

    /** Columns handled outside the source document (metadata and the physical row-id column). */
    private static final Set<String> NON_SOURCE_COLUMNS = Set.of(
        "_id",
        "_seq_no",
        "_version",
        "_primary_term",
        "_routing",
        "_index",
        "_ignored",
        "_source",
        DocumentInput.ROW_ID_FIELD
    );

    private final MapperService mapperService;
    private final String indexName;

    public MvSecondaryRebuilder(MapperService mapperService, String indexName) {
        this.mapperService = mapperService;
        this.indexName = indexName;
    }

    /**
     * Rebuilds the merged-segment content for one secondary format.
     *
     * @param secondaryEngine  the secondary format's indexing engine
     * @param rowsJson         the folded primary rows as a JSON array (row-id order)
     * @param mergedGeneration the writer generation of the merged output segment
     * @return the flushed file set holding exactly the folded rows, at the merged generation
     * @throws IOException if parsing, writing, or flushing fails
     */
    public <P extends DocumentInput<?>> WriterFileSet rebuild(
        IndexingExecutionEngine<?, P> secondaryEngine,
        String rowsJson,
        long mergedGeneration
    ) throws IOException {
        List<Object> rows = parseRows(rowsJson);
        if (rows.isEmpty()) {
            throw new IOException("MV secondary rebuild: folded primary output has no rows");
        }
        DocumentMapper docMapper = mapperService.documentMapper();
        Writer<P> writer = secondaryEngine.createWriter(new WriterConfig(mergedGeneration));
        try {
            long rowId = 0;
            for (Object rowObj : rows) {
                @SuppressWarnings("unchecked")
                Map<String, Object> row = (Map<String, Object>) rowObj;
                P input = secondaryEngine.newDocumentInput();
                buildDocument(docMapper, row, input, rowId);
                WriteResult result = writer.addDoc(input);
                if (result instanceof WriteResult.Failure failure) {
                    throw new IOException("MV secondary rebuild: rejected folded row " + rowId, failure.cause());
                }
                rowId++;
            }
            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet fileSet = fileInfos.getWriterFileSet(secondaryEngine.getDataFormat())
                .orElseThrow(() -> new IOException("MV secondary rebuild: flush produced no file set"));
            if (fileSet.numRows() != rows.size()) {
                throw new IOException(
                    "MV secondary rebuild: flushed " + fileSet.numRows() + " rows but folded output has " + rows.size()
                );
            }
            logger.debug(
                "MV secondary rebuild: [{}] rebuilt {} folded rows at generation {}",
                secondaryEngine.getDataFormat().name(),
                rows.size(),
                mergedGeneration
            );
            return fileSet;
        } finally {
            try {
                writer.close();
            } catch (Exception e) {
                logger.warn("MV secondary rebuild: failed to close rebuild writer", e);
            }
        }
    }

    /** Parses one folded row into the per-format document input, mirroring the live write path. */
    private void buildDocument(DocumentMapper docMapper, Map<String, Object> row, DocumentInput<?> input, long rowId) throws IOException {
        String id = decodeId(row);
        long seqNo = requiredLong(row, SeqNoFieldMapper.NAME);
        long version = requiredLong(row, VersionFieldMapper.NAME);
        long primaryTerm = requiredLong(row, SeqNoFieldMapper.PRIMARY_TERM_NAME);
        String routing = row.get("_routing") instanceof String s ? s : null;

        // Mapped source fields (keys, state columns, other passthroughs).
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (NON_SOURCE_COLUMNS.contains(entry.getKey()) || entry.getValue() == null) {
                    continue;
                }
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            SourceToParse source = new SourceToParse(indexName, id, BytesReference.bytes(builder), XContentType.JSON, routing);
            ParsedDocument parsed = docMapper.parse(source, input);
            assert parsed.getDocumentInput() == input;
        }

        // Metadata mirrored from the engine's live indexing path (DataFormatAwareEngine).
        input.addField(mapperService.fieldType(VersionFieldMapper.NAME), version);
        input.addField(mapperService.fieldType(SeqNoFieldMapper.NAME), seqNo);
        input.addField(PrimaryTermFieldType.INSTANCE, primaryTerm);
        input.setRowId(DocumentInput.ROW_ID_FIELD, rowId);
    }

    private List<Object> parseRows(String rowsJson) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, rowsJson)
        ) {
            return parser.list();
        }
    }

    /** The primary stores {@code _id} as the Uid-encoded bytes; the JSON carries them base64'd. */
    private static String decodeId(Map<String, Object> row) throws IOException {
        Object raw = row.get("_id");
        if (raw instanceof String s) {
            return Uid.decodeId(Base64.getDecoder().decode(s));
        }
        throw new IOException("MV secondary rebuild: folded row is missing the _id passthrough column");
    }

    private static long requiredLong(Map<String, Object> row, String column) throws IOException {
        if (row.get(column) instanceof Number n) {
            return n.longValue();
        }
        throw new IOException("MV secondary rebuild: folded row is missing the [" + column + "] passthrough column");
    }
}
