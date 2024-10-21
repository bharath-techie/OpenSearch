/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a.java
 * compatible open source license.
 */

package org.opensearch.flight;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.common.settings.Settings;

public class FlightServiceTests extends OpenSearchTestCase {

    private FlightService flightService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder().build();
        flightService = new FlightService(settings);
    }

}
