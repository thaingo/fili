// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.web.util.BardConfigResources;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.List;
import java.util.Optional;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.PathSegment;

/**
 * An interface for a factory used to build DataApiRequests.
 */
public interface DataApiRequestFactory {

    /**
     * A default method implementing the same pattern as the classic DataApiRequest constructor.
     *
     * @param tableName  The name of the logical table
     * @param granularity  The granularity of the logical table
     * @param dimensions  The grouping dimensions
     * @param logicalMetrics  The reporting metrics
     * @param intervals  The reporting intervals
     * @param apiFilters  The dimension filters
     * @param havings  The having clause
     * @param sorts  The sort columns
     * @param count  The row limit
     * @param topN  The top n per bucket limit
     * @param format  The response format
     * @param timeZoneId  The name of the timezone
     * @param asyncAfter  The period to asycn after
     * @param perPage  The rows per page
     * @param page  The page number
     * @param requestContext  The request context
     * @param bardConfigResources  The configuration
     *
     * @return  An api request with bound and valid domain objects.
     */
    default DataApiRequest buildDataApiRequest(
            String tableName,
            String granularity,
            List<PathSegment> dimensions,
            String logicalMetrics,
            String intervals,
            String apiFilters,
            String havings,
            String sorts,
            String count,
            String topN,
            String format,
            String timeZoneId,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page,
            ContainerRequestContext requestContext,
            BardConfigResources bardConfigResources) {
        return buildDataApiRequest(
                new DataApiRequestModel(
                        tableName,
                        granularity,
                        dimensions,
                        logicalMetrics,
                        intervals,
                        apiFilters,
                        havings,
                        sorts,
                        count,
                        topN,
                        format,
                        timeZoneId
                ),
                asyncAfter,
                Optional.of(DefaultOutputFormatGenerators.generatePaginationParameters(perPage, page)),
                requestContext,
                bardConfigResources.getMetricDictionary()
        );
    }

    /**
     * Build an api request.
     *
     * @param model  A description of the URI request parameters
     * @param asyncAfter  The time until asynchronous processing
     * @param paginationParameters  The pagination parameters
     * @param requestContext  The request context
     * @param metricDictionary  The dictionary of metrics for this query
     *
     * @return  An api request with bound and valid domain objects.
     */
    DataApiRequest buildDataApiRequest(
            DataApiRequestModel model,
            String asyncAfter,
            Optional<PaginationParameters> paginationParameters,
            ContainerRequestContext requestContext,
            MetricDictionary metricDictionary
    );
}
