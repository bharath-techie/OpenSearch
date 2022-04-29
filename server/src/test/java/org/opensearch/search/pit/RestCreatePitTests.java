/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pit;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CreatePitTestsIT extends OpenSearchRestTestCase {

    @Before
    public void indexDocuments() throws IOException {
        {
            {
                Request doc1 = new Request(HttpPut.METHOD_NAME, "/test/_doc/1");
                doc1.setJsonEntity("{\"id\":1, \"num\":10, \"num2\":50}");
                client().performRequest(doc1);
                Request doc2 = new Request(HttpPut.METHOD_NAME, "/test/_doc/2");
                doc2.setJsonEntity("{ \"id\":2, \"num\":20, \"num2\":40}");
                client().performRequest(doc2);
                Request doc3 = new Request(HttpPut.METHOD_NAME, "/test/_doc/3");
                doc3.setJsonEntity("{ \"id\":3, \"num\":50, \"num2\":35}");
                client().performRequest(doc3);
                Request doc4 = new Request(HttpPut.METHOD_NAME, "/test/_doc/4");
                doc4.setJsonEntity("{ \"id\":4, \"num\":100, \"num2\":10}");
                client().performRequest(doc4);
                Request doc5 = new Request(HttpPut.METHOD_NAME, "/test/_doc/5");
                doc5.setJsonEntity("{ \"id\":5, \"num\":100, \"num2\":10}");
                client().performRequest(doc5);
            }

            {
                Request doc6 = new Request(HttpPut.METHOD_NAME, "/test1/_doc/1");
                doc6.setJsonEntity("{ \"id\":1, \"num\":10, \"num2\":50}");
                client().performRequest(doc6);
            }
        }
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));
    }

    public void testRestCreatePit() throws Exception {
        final Request request = new Request("POST", "/test/_search/point_in_time");
        Map<String, String> params = new HashMap<>();
        params.put("keep_alive", "1m");
        params.put("allow_partial_pit_creation", "false");
        request.addParameters(params);
        Response response = client().performRequest(request);
        assertEquals(response.toString(), "1");
    }
}
