// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import java.util.List;

import javax.ws.rs.core.PathSegment;

/**
 * A bean to capture arguments describing a data api request.
 */
public class DataApiRequestModel {

    /**
     * Constructor.
     *
     * @param tableName  The logical table name
     * @param timeGrain  The logical table granularity
     * @param dimensions  The grouping dimensions
     * @param metrics  The logical metric names
     * @param intervals  The reporting intervals requested
     * @param filters  The api filters
     * @param havings  The havings constraints
     * @param sorts  The sort columns
     * @param count  The row count limit
     * @param topN  The per time bucket top N limit
     * @param format  The reporting format
     * @param timeZone  The time zone name
     */
    public DataApiRequestModel(
            final String tableName,
            final String timeGrain,
            List<PathSegment> dimensions,
            final String metrics,
            final String intervals,
            final String filters,
            final String havings,
            final String sorts,
            final String count,
            final String topN,
            final String format,
            final String timeZone
    ) {
        this.tableName = tableName;
        this.timeGrain = timeGrain;
        this.dimensions = dimensions;
        this.metrics = metrics;
        this.intervals = intervals;
        this.filters = filters;
        this.havings = havings;
        this.sorts = sorts;
        this.count = count;
        this.topN = topN;
        this.format = format;
        this.timeZone = timeZone;
    }

    private final String tableName;
    private final String timeGrain;
    private final List<PathSegment> dimensions;
    private final String metrics;
    private final String intervals;
    private final String filters;
    private final String havings;
    private final String sorts;
    private final String count;
    private final String topN;
    private final String format;
    private final String timeZone;

    public String getTableName() {
        return tableName;
    }

    public String getTimeGrain() {
        return timeGrain;
    }

    public List<PathSegment> getDimensions() {
        return dimensions;
    }

    public String getMetrics() {
        return metrics;
    }

    public String getIntervals() {
        return intervals;
    }

    public String getFilters() {
        return filters;
    }

    public String getHavings() {
        return havings;
    }

    public String getSorts() {
        return sorts;
    }

    public String getCount() {
        return count;
    }

    public String getTopN() {
        return topN;
    }

    public String getFormat() {
        return format;
    }

    public String getTimeZone() {
        return timeZone;
    }
}
