/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http2.HttpVersionPolicy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Abstract base class for all analytics REST integration tests in the sandbox QA package.
 * <p>
 * Handles cluster-level concerns: preserving cluster/indices across test methods,
 * loading classpath resources, JSON escaping, and common assertion helpers.
 * <p>
 * Test data provisioning is handled separately by dataset-specific helpers
 * (e.g. {@link ClickBenchTestHelper}) to keep cluster config orthogonal to test data.
 */
public abstract class AnalyticsRestTestCase extends OpenSearchRestTestCase {

    protected static final Logger logger = LogManager.getLogger(AnalyticsRestTestCase.class);

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    // ── External-cluster support ─────────────────────────────────────────────
    // The default integTest clusters run plain HTTP with no auth, so these hooks
    // are inert there (getProtocol stays "http", no Authorization header, default
    // client builder). They engage only when the corresponding -D system properties
    // are set, letting a curated subset of read-only PPL ITs run against a secured
    // managed domain via:
    //   ./gradlew :sandbox:qa:analytics-engine-rest:restTest -PrestCluster=<host>:443 \
    //     -Dtests.rest.protocol=https \
    //     -Dtests.rest.cluster.username=<user> -Dtests.rest.cluster.password=<pass>
    //
    // For https we force HTTP/1.1: the async client otherwise negotiates HTTP/2 via
    // ALPN, and the managed domain's load balancer returns 400 Bad Request for the
    // framework's HTTP/2-framed requests (e.g. the GET _nodes/plugins that initClient
    // issues). AWS OpenSearch Service domains present publicly-trusted certs, so no
    // trust override is needed; pass -Dtests.rest.trust_all_certs=true only when the
    // target uses a self-signed cert.

    @Override
    protected String getProtocol() {
        return System.getProperty("tests.rest.protocol", "http");
    }

    /**
     * Pre-initialize the REST clients for an external https domain BEFORE the framework's
     * {@code @Before initClient()} runs. JUnit runs a superclass {@code @Before} before any
     * subclass {@code @Before}, so a subclass hook can't pre-empt {@code initClient()} — this
     * must be {@code @BeforeClass} (static, runs first). The framework guards its body with
     * {@code if (client == null)}, so by populating the base class's private static
     * client/version fields here we make {@code initClient()} a no-op — and thereby skip its
     * {@code GET _nodes/plugins} version sniff, which managed OpenSearch Service domains reject
     * with 400 (the {@code _nodes} APIs are restricted). The version is read from {@code GET /},
     * which every domain allows.
     *
     * <p>Engaged only for an external https target ({@code -Dtests.rest.protocol=https}); for
     * the default integTest path this does nothing and the framework initializes normally.
     */
    @BeforeClass
    public static void initExternalDomainClient() throws Exception {
        if (!"https".equals(System.getProperty("tests.rest.protocol", "http"))) {
            return;
        }
        if (getStaticField("client") != null) {
            return; // already initialized for this JVM
        }
        String cluster = System.getProperty("tests.rest.cluster");
        if (cluster == null) {
            return; // let the framework raise its normal "must specify tests.rest.cluster" error
        }
        List<HttpHost> hosts = new ArrayList<>();
        for (String stringUrl : cluster.split(",")) {
            int sep = stringUrl.lastIndexOf(':');
            if (sep < 0) {
                throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
            }
            hosts.add(new HttpHost("https", stringUrl.substring(0, sep), Integer.parseInt(stringUrl.substring(sep + 1))));
        }
        List<HttpHost> clusterHosts = java.util.Collections.unmodifiableList(hosts);
        logger.info("initializing REST clients against {} (external-domain mode)", clusterHosts);
        RestClient client = buildExternalHttpsClient(clusterHosts.toArray(new HttpHost[0]));

        // Version from GET / (managed domains allow this; _nodes/plugins is blocked).
        Map<String, Object> info = entityAsMap(client.performRequest(new Request("GET", "/")));
        @SuppressWarnings("unchecked")
        Map<String, Object> versionInfo = (Map<String, Object>) info.get("version");
        TreeSet<Version> nodeVersions = new TreeSet<>();
        nodeVersions.add(Version.fromString(versionInfo.get("number").toString()));

        setStaticField("clusterHosts", clusterHosts);
        setStaticField("client", client);
        setStaticField("adminClient", client);
        setStaticField("nodeVersions", nodeVersions);
    }

    /**
     * Build an https REST client for an external domain: basic auth from
     * {@code tests.rest.cluster.username/password}, HTTP/1.1 forced (the domain's LB rejects
     * the async client's HTTP/2 framing with 400), and trust-all TLS only when
     * {@code -Dtests.rest.trust_all_certs=true}.
     */
    private static RestClient buildExternalHttpsClient(HttpHost[] hosts) {
        RestClientBuilder builder = RestClient.builder(hosts);
        String user = System.getProperty("tests.rest.cluster.username");
        String pass = System.getProperty("tests.rest.cluster.password");
        if (user != null) {
            String token = Base64.getEncoder()
                .encodeToString((user + ":" + (pass == null ? "" : pass)).getBytes(StandardCharsets.UTF_8));
            builder.setDefaultHeaders(new org.apache.hc.core5.http.Header[] {
                new org.apache.hc.core5.http.message.BasicHeader("Authorization", "Basic " + token) });
        }
        boolean trustAll = Boolean.parseBoolean(System.getProperty("tests.rest.trust_all_certs", "false"));
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            try {
                ClientTlsStrategyBuilder tlsBuilder = ClientTlsStrategyBuilder.create()
                    // See https://issues.apache.org/jira/browse/HTTPCLIENT-2219
                    .setTlsDetailsFactory(new Factory<SSLEngine, TlsDetails>() {
                        @Override
                        public TlsDetails create(final SSLEngine sslEngine) {
                            return new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol());
                        }
                    });
                if (trustAll) {
                    final SSLContext sslContext = SSLContext.getInstance("TLS");
                    sslContext.init(null, new TrustManager[] { TRUST_ALL }, new SecureRandom());
                    tlsBuilder.setSslContext(sslContext).setHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                }
                return httpClientBuilder
                    .setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_1)
                    .setConnectionManager(
                        PoolingAsyncClientConnectionManagerBuilder.create().setTlsStrategy(tlsBuilder.build()).build()
                    );
            } catch (Exception e) {
                throw new RuntimeException("Failed to set up https REST client for external cluster", e);
            }
        });
        return builder.build();
    }

    private static Object getStaticField(String name) throws Exception {
        java.lang.reflect.Field f = OpenSearchRestTestCase.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(null);
    }

    private static void setStaticField(String name, Object value) throws Exception {
        java.lang.reflect.Field f = OpenSearchRestTestCase.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(null, value);
    }

    /**
     * True when the suite is pointed at an external https cluster (managed domain) rather than a
     * self-provisioned local integTest cluster. Driven by {@code -Dtests.rest.protocol=https}.
     */
    protected static boolean isExternalCluster() {
        return "https".equals(System.getProperty("tests.rest.protocol", "http"));
    }

    /**
     * Skip (not fail) the current test when running against an external managed domain. Use in
     * tests that must mutate cluster-wide state the domain forbids — e.g. {@code PUT /_cluster/settings}
     * for concurrent-search mode or oversampling factor, which return 401 on a managed domain.
     */
    protected static void assumeNotExternalCluster(String reason) {
        org.junit.Assume.assumeFalse("Skipped on external managed domain: " + reason, isExternalCluster());
    }

    /** Trust manager that accepts any certificate — used only when -Dtests.rest.trust_all_certs=true. */
    private static final X509TrustManager TRUST_ALL = new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    };

    /**
     * Load a classpath resource as a UTF-8 string.
     * Fails with an assertion error if the resource does not exist.
     */
    protected String loadResource(String path) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            assertNotNull("Resource not found: " + path, is);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                return reader.lines().collect(Collectors.joining("\n"));
            }
        }
    }

    /**
     * Escape backslashes and double quotes for safe embedding in JSON string values.
     */
    protected static String escapeJson(String text) {
        return text.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Assert that the response has HTTP 200 status and return the body as a parsed Map.
     * The {@code context} string is included in failure messages for easier debugging.
     */
    protected Map<String, Object> assertOkAndParse(Response response, String context) throws IOException {
        assertEquals(context + ": expected HTTP 200", 200, response.getStatusLine().getStatusCode());
        return entityAsMap(response);
    }

    /**
     * Extract column names from a PPL response's {@code schema} field. The real opensearch-sql
     * plugin emits {@code "schema": [{"name": "...", "type": "..."}, ...]} (vs. the legacy
     * opensearch-sql shim's bare {@code "columns": [name, ...]}). Returns an empty list
     * if no schema is present.
     */
    @SuppressWarnings("unchecked")
    protected static List<String> extractColumnNames(Map<String, Object> response) {
        Object schema = response.get("schema");
        if (schema == null) {
            return new ArrayList<>();
        }
        List<Map<String, Object>> entries = (List<Map<String, Object>>) schema;
        return entries.stream().map(e -> (String) e.get("name")).collect(Collectors.toList());
    }

    /**
     * Hook invoked before each test method via JUnit's {@code @Before}, and also before
     * each {@link #executePpl} call as a belt-and-braces guard. Subclasses with lazily-
     * provisioned datasets should override to call their {@code DatasetProvisioner.provision}
     * (gated on a static {@code dataProvisioned} flag so the work only happens once per
     * JVM). Default: no-op.
     *
     * <p>Routing through {@code @Before} means setup that doesn't go through
     * {@link #executePpl} (alias creation, raw {@code _search}, expect-failure paths) still
     * sees the dataset present.
     */
    protected void onBeforeQuery() throws IOException {}

    @Before
    public final void invokeOnBeforeQueryHook() throws IOException {
        onBeforeQuery();
    }

    /**
     * Execute a PPL query against the real opensearch-sql plugin at {@code /_plugins/_ppl},
     * asserting HTTP 200 and returning the parsed JSON body. The {@link #onBeforeQuery}
     * hook has already fired via {@code @Before}, so subclasses don't need to ensure data
     * provisioning here.
     */
    protected Map<String, Object> executePpl(String ppl) throws IOException {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    /**
     * Execute a PPL query against the real opensearch-sql plugin at {@code /_plugins/_ppl} with
     * {@code "profile": true}, returning the parsed body. When profiling is enabled the production
     * plugin executes the query and attaches the analytics-engine {@code profile} block — the same
     * structure ({@code full_plan}, {@code stages[].execution_type}, {@code tasks[].physical_plan},
     * {@code mode=[PARTIAL]/[FINAL]}, {@code chosen_backend}) that the {@code test-ppl-frontend}
     * shim's {@code /_analytics/ppl/_explain} surfaces. Use this (not the shim) so plan-shape tests
     * run against any cluster, including managed domains that don't have the shim installed.
     *
     * <p>{@code rows} is mirrored to {@code datarows} for assertion helpers that read either.
     */
    protected Map<String, Object> executePplWithProfile(String ppl) throws IOException {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\", \"profile\": true}");
        Response response = client().performRequest(request);
        Map<String, Object> parsed = assertOkAndParse(response, "PPL (profile): " + ppl);
        if (parsed.containsKey("datarows") && parsed.containsKey("rows") == false) {
            parsed.put("rows", parsed.get("datarows"));
        }
        return parsed;
    }

    /**
     * Execute a SQL query against the real opensearch-sql plugin at {@code /_plugins/_sql},
     * asserting HTTP 200 and returning the parsed JSON body. Same {@code @Before} provisioning
     * hook flow as {@link #executePpl(String)} — subclasses don't need to ensure datasets here.
     */
    protected Map<String, Object> executeSql(String sql) throws IOException {
        Request request = new Request("POST", "/_plugins/_sql");
        request.setJsonEntity("{\"query\": \"" + escapeJson(sql) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "SQL: " + sql);
    }

    /**
     * Execute a PPL query against the {@code test-ppl-frontend} shim at {@code /_analytics/ppl}.
     * Use this for tests that exercise <em>engine-internal</em> behavior (e.g. perf-delegation
     * marker placement, explain output shape) where the opensearch-sql plugin's user-facing PPL
     * surface isn't on the hook. Tests that exercise a real user-typed PPL feature should keep
     * using {@link #executePpl(String)} so they validate the production path end-to-end.
     *
     * <p>On an external managed domain the shim plugin isn't installed ({@code /_analytics/ppl}
     * returns 401), so this transparently routes to the real {@code /_plugins/_ppl} with
     * {@code profile:true}. That response carries the same {@code rows}/{@code profile} structure
     * the shim emits, so row- and profile-reading callers work unchanged. Both responses are
     * normalized so {@code rows} and {@code datarows} are interchangeable.
     */
    protected Map<String, Object> executePplViaShim(String ppl) throws IOException {
        if (isExternalCluster()) {
            return executePplWithProfile(ppl); // already mirrors datarows -> rows
        }
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        Map<String, Object> parsed = assertOkAndParse(response, "PPL (shim): " + ppl);
        // Normalize: shim returns {columns, rows}; real SQL plugin returns {schema, datarows}.
        // Tests that share assertions across both helpers read 'datarows' — mirror it for shim.
        if (parsed.containsKey("rows") && parsed.containsKey("datarows") == false) {
            parsed.put("datarows", parsed.get("rows"));
        }
        return parsed;
    }
}
