/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap.server;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlightServerBuilderTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private BufferAllocator allocator;
    private FlightProducer producer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ServerConfig.init(Settings.EMPTY);
        threadPool = mock(ThreadPool.class);
        allocator = new RootAllocator();
        producer = mock(FlightProducer.class);
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testBuilderConstructorWithValidInputs() throws IOException {
        FlightServerBuilder newBuilder = new FlightServerBuilder(threadPool, () -> allocator, producer, mock(SslContextProvider.class));
        assertNotNull(newBuilder);
        assertNotNull(newBuilder.build());
    }

    public void testBuilderConstructorWithNullThreadPool() {
        expectThrows(
            NullPointerException.class,
            () -> (new FlightServerBuilder(null, () -> allocator, producer, mock(SslContextProvider.class))).build()
        );
    }

    public void testBuilderConstructorWithNullAllocator() {
        expectThrows(
            NullPointerException.class,
            () -> (new FlightServerBuilder(threadPool, null, producer, mock(SslContextProvider.class))).build()
        );
    }

    public void testBuilderConstructorWithSslNull() {
        SslContextProvider sslContextProvider = mock(SslContextProvider.class);
        when(sslContextProvider.isSslEnabled()).thenReturn(true);
        when(sslContextProvider.getServerSslContext()).thenReturn(null);
        FlightServerBuilder newBuilder = new FlightServerBuilder(threadPool, () -> allocator, producer, sslContextProvider);
        assertNotNull(newBuilder);
        expectThrows(NullPointerException.class, newBuilder::build);
    }
}
