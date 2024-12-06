/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

public class AnaylzeTest extends OpenSearchSingleNodeTestCase {
    private static final String INDEX_NAME = "test4";

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .build();
    }

    public void testCreateIndexWithCustomMappingAndIngestDocument() throws IOException {
        // Create synonyms file in temp directory
        Path synonymsFile = createTempFile("synonyms", ".txt");
        Files.write(synonymsFile, "test, examination\nquiz, test".getBytes(StandardCharsets.UTF_8));

        // Prepare settings with synonyms file path
        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", 5)
            .put("index.number_of_replicas", 1)
            .put("index.max_result_window", 50000)
            .put("index.analysis.analyzer.basic_analyzer.tokenizer", "standard")
            .put("index.analysis.analyzer.basic_analyzer.filter", Arrays.toString(new String[]{"asciifolding", "lowercase", "synonym_filter"}))
            .put("index.analysis.analyzer.custom_analyzer.tokenizer", "standard")
            .put("index.analysis.analyzer.custom_analyzer.filter",
                Arrays.toString(new String[]{"asciifolding", "lowercase", "synonym_filter", "autocomplete_filter"}))
            .put("index.analysis.filter.synonym_filter.type", "synonym")
            .put("index.analysis.filter.synonym_filter.synonyms_path", "synonyms.txt")
            .put("index.analysis.filter.autocomplete_filter.type", "edge_ngram")
            .put("index.analysis.filter.autocomplete_filter.min_gram", 3)
            .put("index.analysis.filter.autocomplete_filter.max_gram", 20)
            .build();

        // Create index with settings and mapping
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME);
        createIndexRequest.settings(indexSettings);

        // Your mapping JSON
        String mapping = "{\n" +
            "\"mappings\": {\n" +
            "  \"properties\": {\n" +
            "    \"@timestamp\": {\n" +
            "      \"type\": \"date\"\n" +
            "    },\n" +
            "    \"app_id\": {\n" +
            "      \"type\": \"keyword\",\n" +
            "      \"normalizer\": \"custom_normalizer\"\n" +
            "    },\n" +
            "    \"asset_type\": {\n" +
            "      \"type\": \"keyword\",\n" +
            "      \"normalizer\": \"custom_normalizer\"\n" +
            "    },\n" +
            "    \"date_created\": {\n" +
            "      \"type\": \"date\"\n" +
            "    },\n" +
            "    \"date_updated\": {\n" +
            "      \"type\": \"date\"\n" +
            "    },\n" +
            "    \"dependencies\": {\n" +
            "      \"type\": \"nested\",\n" +
            "      \"properties\": {\n" +
            "        \"ids\": {\n" +
            "          \"type\": \"keyword\",\n" +
            "          \"normalizer\": \"custom_normalizer\"\n" +
            "        },\n" +
            "        \"type\": {\n" +
            "          \"type\": \"keyword\",\n" +
            "          \"normalizer\": \"custom_normalizer\"\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"description\": {\n" +
            "      \"type\": \"text\",\n" +
            "      \"boost\": 2.0,\n" +
            "      \"fields\": {\n" +
            "        \"keyword\": {\n" +
            "          \"type\": \"keyword\",\n" +
            "          \"normalizer\": \"custom_normalizer\"\n" +
            "        }\n" +
            "      },\n" +
            "      \"analyzer\": \"basic_analyzer\",\n" +
            "      \"index_phrases\": true\n" +
            "    },\n" +
            "    \"domain\": {\n" +
            "      \"type\": \"nested\",\n" +
            "      \"properties\": {\n" +
            "        \"code_type\": {\n" +
            "          \"type\": \"keyword\",\n" +
            "          \"normalizer\": \"custom_normalizer\"\n" +
            "        },\n" +
            "        \"code_type_ext\": {\n" +
            "          \"type\": \"keyword\",\n" +
            "          \"normalizer\": \"custom_normalizer\"\n" +
            "        },\n" +
            "        \"counts\": {\n" +
            "          \"type\": \"integer\"\n" +
            "        },\n" +
            "        \"id_search\": {\n" +
            "          \"type\": \"text\",\n" +
            "          \"boost\": 4.0,\n" +
            "          \"fields\": {\n" +
            "            \"keyword\": {\n" +
            "              \"type\": \"keyword\",\n" +
            "              \"normalizer\": \"custom_normalizer\"\n" +
            "            },\n" +
            "            \"ngram\": {\n" +
            "              \"type\": \"text\",\n" +
            "              \"analyzer\": \"custom_analyzer\"\n" +
            "            },\n" +
            "            \"no_whitespace\": {\n" +
            "              \"type\": \"text\",\n" +
            "              \"analyzer\": \"no_whitespace_analyzer\"\n" +
            "            }\n" +
            "          },\n" +
            "          \"analyzer\": \"basic_analyzer\"\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"id\": {\n" +
            "      \"type\": \"keyword\",\n" +
            "      \"normalizer\": \"custom_normalizer\"\n" +
            "    },\n" +
            "    \"name\": {\n" +
            "      \"type\": \"text\",\n" +
            "      \"boost\": 3.0,\n" +
            "      \"fields\": {\n" +
            "        \"keyword\": {\n" +
            "          \"type\": \"keyword\",\n" +
            "          \"normalizer\": \"custom_normalizer\"\n" +
            "        },\n" +
            "        \"ngram\": {\n" +
            "          \"type\": \"text\",\n" +
            "          \"analyzer\": \"custom_analyzer\"\n" +
            "        },\n" +
            "        \"no_whitespace\": {\n" +
            "          \"type\": \"text\",\n" +
            "          \"analyzer\": \"no_whitespace_analyzer\"\n" +
            "        }\n" +
            "      },\n" +
            "      \"analyzer\": \"basic_analyzer\",\n" +
            "      \"index_phrases\": true\n" +
            "    },\n" +
            "    \"status\": {\n" +
            "      \"type\": \"keyword\",\n" +
            "      \"normalizer\": \"custom_normalizer\"\n" +
            "    },\n" +
            "    \"tenant_id\": {\n" +
            "      \"type\": \"keyword\"\n" +
            "    },\n" +
            "    \"type\": {\n" +
            "      \"type\": \"nested\",\n" +
            "      \"properties\": {\n" +
            "        \"id_normalized\": {\n" +
            "          \"type\": \"text\",\n" +
            "          \"boost\": 4.0,\n" +
            "          \"fields\": {\n" +
            "            \"keyword\": {\n" +
            "              \"type\": \"keyword\",\n" +
            "              \"normalizer\": \"custom_normalizer\"\n" +
            "            },\n" +
            "            \"ngram\": {\n" +
            "              \"type\": \"text\",\n" +
            "              \"analyzer\": \"custom_analyzer\"\n" +
            "            },\n" +
            "            \"no_whitespace\": {\n" +
            "              \"type\": \"text\",\n" +
            "              \"analyzer\": \"no_whitespace_analyzer\"\n" +
            "            }\n" +
            "          },\n" +
            "          \"analyzer\": \"basic_analyzer\"\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"user_id\": {\n" +
            "      \"type\": \"keyword\"\n" +
            "    }\n" +
            "  }\n" +
            "}\n" +
            "}";
        createIndexRequest.mapping(mapping, XContentType.JSON);

        client().admin().indices().create(createIndexRequest).actionGet();

        // Index a document
        String document = "{\"asset_type\":\"code\"," +
            "\"id\":\"0001A\"," +
            "\"name\":\"Intramuscular administration of single severe acute respiratory syndrome coronavirus 2 (COVID-19) vaccine\"," +
            "\"status\":\"active\"," +
            "\"domain\":{\"code_type\":\"procedure\",\"code_type_ext\":\"HCPT\",\"counts\":0,\"id_search\":\"0001A\"}," +
            "\"type\":{\"id_normalized\":\"0001A\"}," +
            "\"description\":\"\"}";

        IndexRequest indexRequest = new IndexRequest(INDEX_NAME)
            .source(document, XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE);

        IndexResponse indexResponse = client().index(indexRequest).actionGet();
        assertThat(indexResponse.status(), equalTo(RestStatus.CREATED));

        // Verify document count
        SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).get();
        assertHitCount(searchResponse, 1);
    }
}
