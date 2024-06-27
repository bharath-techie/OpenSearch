/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.test.OpenSearchTestCase;

public class MetricAggregatorInfoTests extends OpenSearchTestCase {

    public void testConstructor() {
        MetricAggregatorInfo pair = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column1",
            "star_tree_field",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        assertEquals(MetricStat.SUM, pair.getMetricStat());
        assertEquals("column1", pair.getField());
    }

    public void testCountStarConstructor() {
        MetricAggregatorInfo pair = new MetricAggregatorInfo(
            MetricStat.COUNT,
            "anything",
            "star_tree_field",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        assertEquals(MetricStat.COUNT, pair.getMetricStat());
        assertEquals("anything", pair.getField());
    }

    public void testToFieldName() {
        MetricAggregatorInfo pair = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column2",
            "star_tree_field",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        assertEquals("star_tree_field_column2_sum", pair.toFieldName());
    }

    public void testEquals() {
        MetricAggregatorInfo pair1 = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column1",
            "star_tree_field",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        MetricAggregatorInfo pair2 = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column1",
            "star_tree_field",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        assertEquals(pair1, pair2);
        assertNotEquals(
            pair1,
            new MetricAggregatorInfo(MetricStat.COUNT, "column1", "star_tree_field", IndexNumericFieldData.NumericType.DOUBLE, null)
        );
        assertNotEquals(
            pair1,
            new MetricAggregatorInfo(MetricStat.SUM, "column2", "star_tree_field", IndexNumericFieldData.NumericType.DOUBLE, null)
        );
    }

    public void testHashCode() {
        MetricAggregatorInfo pair1 = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column1",
            "star_tree_field",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        MetricAggregatorInfo pair2 = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column1",
            "star_tree_field",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        assertEquals(pair1.hashCode(), pair2.hashCode());
    }

    public void testCompareTo() {
        MetricAggregatorInfo pair1 = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column1",
            "star_tree_field",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        MetricAggregatorInfo pair2 = new MetricAggregatorInfo(
            MetricStat.SUM,
            "column2",
            "star_tree_field",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        MetricAggregatorInfo pair3 = new MetricAggregatorInfo(
            MetricStat.COUNT,
            "column1",
            "star_tree_field",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        assertTrue(pair1.compareTo(pair2) < 0);
        assertTrue(pair2.compareTo(pair1) > 0);
        assertTrue(pair1.compareTo(pair3) > 0);
        assertTrue(pair3.compareTo(pair1) < 0);
    }
}
