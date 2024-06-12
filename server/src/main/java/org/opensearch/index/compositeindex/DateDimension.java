/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import java.io.IOException;
import org.opensearch.common.Rounding;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.Mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Date dimension class
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DateDimension extends Dimension {
    private final List<Rounding.DateTimeUnit> calendarIntervals;

    public DateDimension(String name, Settings settings, CompositeIndexConfig compositeIndexConfig) {
        super(name);
        List<String> intervalStrings = settings.getAsList("calendar_interval");
        if (intervalStrings == null || intervalStrings.isEmpty()) {
            this.calendarIntervals = compositeIndexConfig.getDefaultDateIntervals();
        } else {
            this.calendarIntervals = new ArrayList<>();
            for (String interval : intervalStrings) {
                this.calendarIntervals.add(CompositeIndexConfig.getTimeUnit(interval));
            }
        }
    }

    public DateDimension(String name, List<Rounding.DateTimeUnit> intervals) {
        super(name);
        this.calendarIntervals = intervals;
    }

    @SuppressWarnings("unchecked")
    public DateDimension(Map.Entry<String, Object> dimension, Mapper.TypeParser.ParserContext c) {
        super(dimension.getKey());
        List<String> intervalStrings = XContentMapValues.extractRawValues("calendar_interval", (Map<String, Object>) dimension.getValue())
            .stream()
            .map(Object::toString)
            .collect(Collectors.toList());
        if (intervalStrings == null || intervalStrings.isEmpty()) {
            this.calendarIntervals = CompositeIndexConfig.DEFAULT_DATE_INTERVALS.get(c.getSettings());
        } else {
            this.calendarIntervals = new ArrayList<>();
            for (String interval : intervalStrings) {
                this.calendarIntervals.add(CompositeIndexConfig.getTimeUnit(interval));
            }
        }
    }

    public void setCalendarIntervals() {
        this.calendarIntervals.clear();
        this.calendarIntervals.add(CompositeIndexConfig.getTimeUnit("1d"));
    }

    public List<Rounding.DateTimeUnit> getIntervals() {
        return calendarIntervals;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params)
        throws IOException {
        builder.startObject(this.getField());
        builder.field("type", "date");
        builder.startArray("calendar_interval");
        for (Rounding.DateTimeUnit interval : calendarIntervals) {
            builder.value(interval.shortName());
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
