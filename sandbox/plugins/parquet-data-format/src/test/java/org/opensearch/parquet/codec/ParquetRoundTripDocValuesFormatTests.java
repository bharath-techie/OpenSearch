/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.parquet.bridge.RustBridge;

import java.nio.file.Files;

/**
 * Runs Lucene's exhaustive {@link BaseDocValuesFormatTestCase} contract battery (randomized
 * values, missing docs, advance/advanceExact semantics, merges, huge segments) against the REAL
 * Parquet doc-values read stack via {@link RoundTripParquetDocValuesFormat}: NUMERIC, BINARY and
 * SORTED_NUMERIC fields round-trip through a genuine Parquet file and are read back through
 * {@link ParquetDocValuesProducer} on the DataFusion decode path.
 *
 * <p>SORTED / SORTED_SET fields delegate to Lucene90 inside the round-trip format: without the
 * composite engine's sidecar terms index the Parquet sorted path is deliberately fail-fast
 * (see the tiered-ordinals design doc), so those tests exercise the delegate, not our stack.
 */
public class ParquetRoundTripDocValuesFormatTests extends BaseDocValuesFormatTestCase {

    private Codec codec;

    @Override
    protected Codec getCodec() {
        if (codec == null) {
            RustBridge.initLogger();
            // This branch still defaults to the legacy codec_native decode path; the production
            // configuration under test is the DataFusion path.
            ParquetDocValuesProducer.setDecodePath(org.opensearch.parquet.ParquetSettings.DECODE_PATH_DATAFUSION);
            try {
                RoundTripParquetDocValuesFormat.SPILL_DIR = Files.createTempDirectory("parquet-dv-roundtrip");
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
            // Randomize the tier boundary so the battery exercises BOTH ordinal tiers:
            // 0 forces every sorted field onto disk-backed uninverted ordinals; larger values
            // keep low-cardinality fields on the heap dictionary tier.
            RoundTripParquetDocValuesFormat.DICTIONARY_MAX_TERMS = org.apache.lucene.tests.util.LuceneTestCase.random()
                .nextBoolean() ? 0 : 65536;
            codec = TestUtil.alwaysDocValuesFormat(new RoundTripParquetDocValuesFormat(RoundTripParquetDocValuesFormat.SPILL_DIR));
        }
        return codec;
    }
}
