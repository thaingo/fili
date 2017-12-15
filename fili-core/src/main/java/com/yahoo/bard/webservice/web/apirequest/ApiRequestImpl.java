// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRICS_NOT_IN_TABLE;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.filters.ApiFilters;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * API Request. Abstract class offering default implementations for the common components of API request objects.
 */
public abstract class ApiRequestImpl implements ApiRequest {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final Logger LOG = LoggerFactory.getLogger(ApiRequestImpl.class);


    protected static final String COMMA_AFTER_BRACKET_PATTERN = "(?<=]),";

    private static final int DEFAULT_PER_PAGE = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("default_per_page")
    );
    private static final int DEFAULT_PAGE = 1;
    private static final PaginationParameters DEFAULT_PAGINATION = new PaginationParameters(
            DEFAULT_PER_PAGE,
            DEFAULT_PAGE
    );
    private static final String SYNCHRONOUS_REQUEST_FLAG = "never";

    protected final ResponseFormatType format;
    PaginationHelper paginationHelper;
    protected final UriInfo uriInfo;
    protected final Response.ResponseBuilder builder;
    protected Pagination<?> pagination;
    protected final long asyncAfter;

    /**
     * Parses the API request URL and generates the API request object.
     *
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param asyncAfter  How long the user is willing to wait for a synchronous request in milliseconds, if null
     * defaults to the system config {@code default_asyncAfter}
     * @param perPage  number of rows to display per page of results. If present in the original request, must be a
     * positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive integer. If not
     * present, must be the empty string.
     * @param uriInfo  The URI of the request object.
     *
     * @throws BadApiRequestException if pagination parameters in the API request are not positive integers.
     */
    public ApiRequestImpl(
            String format,
            String asyncAfter,
            @NotNull String perPage,
            @NotNull String page,
            UriInfo uriInfo
    ) throws BadApiRequestException {
        this.uriInfo = uriInfo;
        this.format = generateAcceptFormat(format);
        this.builder = Response.status(Response.Status.OK);
        DefaultOutputFormatGenerators.generatePaginationParameters(perPage, page);
        this.paginationHelper = new PaginationHelper(uriInfo, perPage, page, builder);
        this.asyncAfter = DefaultOutputFormatGenerators.generateAsyncAfter(
                asyncAfter == null ?
                        SYSTEM_CONFIG.getStringProperty(SYSTEM_CONFIG.getPackageVariableName("default_asyncAfter")) :
                        asyncAfter
        );

    }

    /**
     * Parses the API request URL and generates the API request object. Defaults asyncAfter to never.
     *
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param perPage  number of rows to display per page of results. If present in the original request, must be a
     * positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive integer. If not
     * present, must be the empty string.
     * @param uriInfo  The URI of the request object.
     *
     * @throws BadApiRequestException if pagination parameters in the API request are not positive integers.
     */
    public ApiRequestImpl(
            String format,
            @NotNull String perPage,
            @NotNull String page,
            UriInfo uriInfo
    ) throws BadApiRequestException {
        this(format, SYNCHRONOUS_REQUEST_FLAG, perPage, page, uriInfo);
    }

    /**
     * All argument constructor, meant to be used for rewriting apiRequest.
     *
     * @param format  The format of the response
     * @param asyncAfter  How long the user is willing to wait for a synchronous request, in milliseconds
     * @param paginationParameters  The parameters used to describe pagination
     * @param uriInfo  The uri details
     * @param builder  The response builder for this request
     */
    protected ApiRequestImpl(
            ResponseFormatType format,
            long asyncAfter,
            PaginationParameters paginationParameters,
            UriInfo uriInfo,
            Response.ResponseBuilder builder
    ) {
        this.format = format;
        this.asyncAfter = asyncAfter;
        this.paginationHelper = new PaginationHelper(uriInfo, paginationParameters);
        this.uriInfo = uriInfo;
        this.builder = builder;
    }


    @Override
    public ResponseFormatType getFormat() {
        return format;
    }

    @Override
    public PaginationParameters getPaginationParameters() {
        return paginationHelper.getPaginationParameters();
    }

    @Override
    public UriInfo getUriInfo() {
        return uriInfo;
    }

    @Override
    public Pagination<?> getPagination() {
        return pagination;
    }

    @Override
    public long getAsyncAfter() {
        return asyncAfter;
    }

    /**
     * Generate a Granularity instance based on a path element.
     *
     * @param granularity  A string representation of the granularity
     * @param dateTimeZone  The time zone to use for this granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance with time zone information
     * @throws BadApiRequestException if the string matches no meaningful granularity
     */
    protected Granularity generateGranularity(
            @NotNull String granularity,
            @NotNull DateTimeZone dateTimeZone,
            @NotNull GranularityParser granularityParser
    ) throws BadApiRequestException {
        return DefaultGranularityGenerators.generateGranularity(granularity, dateTimeZone, granularityParser);
    }

    /**
     * Generate a Granularity instance based on a path element.
     *
     * @param granularity  A string representation of the granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance without time zone information
     * @throws BadApiRequestException if the string matches no meaningful granularity
     */
    protected Granularity generateGranularity(String granularity, GranularityParser granularityParser)
            throws BadApiRequestException {
        return DefaultGranularityGenerators.generateGranularity(granularity, granularityParser);
    }

    /**
     * Extracts the list of dimension names from the url dimension path segments and generates a set of dimension
     * objects based on it.
     *
     * @param apiDimensions  Dimension path segments from the URL.
     * @param dimensionDictionary  Dimension dictionary contains the map of valid dimension names and dimension objects.
     *
     * @return Set of dimension objects.
     * @throws BadApiRequestException if an invalid dimension is requested.
     */
    protected LinkedHashSet<Dimension> generateDimensions(
            List<PathSegment> apiDimensions,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        return DefaultDimensionGenerators.generateDimensions(apiDimensions, dimensionDictionary);
    }

    /**
     * Extracts the list of metrics from the url metric query string and generates a set of LogicalMetrics.
     * <p>
     * If the query contains undefined metrics, {@link com.yahoo.bard.webservice.web.BadApiRequestException} will be
     * thrown.
     *
     * @param apiMetricQuery  URL query string containing the metrics separated by ','
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects
     *
     * @return set of metric objects
     */
    protected LinkedHashSet<LogicalMetric> generateLogicalMetrics(
            String apiMetricQuery,
            MetricDictionary metricDictionary

    ) {
        return DefaultLogicalMetricsGenerators.generateLogicalMetrics(apiMetricQuery, metricDictionary);
    }

    /**
     * Validate that all metrics are part of the logical table.
     *
     * @param logicalMetrics  The set of metrics being validated
     * @param table  The logical table for the request
     *
     * @throws BadApiRequestException if the requested metrics are not in the logical table
     */
    protected void validateMetrics(Set<LogicalMetric> logicalMetrics, LogicalTable table)
            throws BadApiRequestException {
        //get metric names from the logical table
        Set<String> validMetricNames = table.getLogicalMetrics().stream()
                .map(LogicalMetric::getName)
                .collect(Collectors.toSet());

        //get metric names from logicalMetrics and remove all the valid metrics
        Set<String> invalidMetricNames = logicalMetrics.stream()
                .map(LogicalMetric::getName)
                .filter(it -> !validMetricNames.contains(it))
                .collect(Collectors.toSet());

        //requested metrics names are not present in the logical table metric names set
        if (!invalidMetricNames.isEmpty()) {
            LOG.debug(METRICS_NOT_IN_TABLE.logFormat(invalidMetricNames, table.getName()));
            throw new BadApiRequestException(
                    METRICS_NOT_IN_TABLE.format(invalidMetricNames, table.getName())
            );
        }
    }

    /**
     * Extracts the set of intervals from the api request.
     *
     * @param apiIntervalQuery  API string containing the intervals in ISO 8601 format, values separated by ','.
     * @param granularity  The granularity to generate the date based on period or macros.
     * @param dateTimeFormatter  The formatter to parse date time interval segments
     *
     * @return Set of jodatime interval objects.
     * @throws BadApiRequestException if the requested interval is not found.
     */
    protected static Set<Interval> generateIntervals(
            String apiIntervalQuery,
            Granularity granularity,
            DateTimeFormatter dateTimeFormatter
    ) throws BadApiRequestException {
        return generateIntervals(new DateTime(), apiIntervalQuery, granularity, dateTimeFormatter);
    }


    /**
     * Extracts the set of intervals from the api request.
     *
     * @param now The 'now' for which time macros will be relatively calculated
     * @param apiIntervalQuery  API string containing the intervals in ISO 8601 format, values separated by ','.
     * @param granularity  The granularity to generate the date based on period or macros.
     * @param dateTimeFormatter  The formatter to parse date time interval segments
     *
     * @return Set of jodatime interval objects.
     * @throws BadApiRequestException if the requested interval is not found.
     */
    protected static Set<Interval> generateIntervals(
            DateTime now,
            String apiIntervalQuery,
            Granularity granularity,
            DateTimeFormatter dateTimeFormatter
    ) throws BadApiRequestException {
        return DateAndTimeGenerators.generateIntervals(apiIntervalQuery, granularity, dateTimeFormatter);
    }

    /**
     * Generates filter objects on the based on the filter query in the api request.
     *
     * @param filterQuery  Expects a URL filter query String in the format:
     * (dimension name).(fieldname)-(operation):[?(value or comma separated values)]?
     * @param table  The logical table for the data request
     * @param dimensionDictionary  DimensionDictionary
     *
     * @return Set of filter objects.
     * @throws BadApiRequestException if the filter query string does not match required syntax, or the filter
     * contains a 'startsWith' or 'contains' operation while the BardFeatureFlag.DATA_STARTS_WITH_CONTAINS_ENABLED is
     * off.
     */
    public ApiFilters generateFilters(
            String filterQuery,
            LogicalTable table,
            DimensionDictionary dimensionDictionary
    ) throws BadApiRequestException {
        return DefaultFilterGenerator.generateFilters(filterQuery, table, dimensionDictionary);
    }

    /**
     * Get datetime from the given input text based on granularity.
     *
     * @param now  current datetime to compute the floored date based on granularity
     * @param granularity  granularity to truncate the given date to.
     * @param dateText  start/end date text which could be actual date or macros
     * @param timeFormatter  a time zone adjusted date time formatter
     *
     * @return joda datetime of the given start/end date text or macros
     *
     * @throws BadApiRequestException if the granularity is "all" and a macro is used
     */
    public static DateTime getAsDateTime(
            DateTime now,
            Granularity granularity,
            String dateText,
            DateTimeFormatter timeFormatter
    ) throws BadApiRequestException {
        return DateAndTimeGenerators.getAsDateTime(now, granularity, dateText, timeFormatter);
    }

    /**
     * Get the timezone for the request.
     *
     * @param timeZoneId  String of the TimeZone ID
     * @param systemTimeZone  TimeZone of the system to use if there is no timeZoneId
     *
     * @return the request's TimeZone
     */
    protected DateTimeZone generateTimeZone(String timeZoneId, DateTimeZone systemTimeZone) {
        return DateAndTimeGenerators.generateTimeZone(timeZoneId, systemTimeZone);
    }


     /**
     * Generates the format in which the response data is expected.
     *
     * @param format  Expects a URL format query String.
     *
     * @return Response format type (CSV or JSON).
     * @throws BadApiRequestException if the requested format is not found.
     */
    protected ResponseFormatType generateAcceptFormat(String format) throws BadApiRequestException {
        return DefaultOutputFormatGenerators.generateAcceptFormat(format);
    }

    /**
     * Extracts a specific logical table object given a valid table name and a valid granularity.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  logical table corresponding to the table name specified in the URL
     * @param logicalTableDictionary  Logical table dictionary contains the map of valid table names and table objects.
     *
     * @return Set of logical table objects.
     * @throws BadApiRequestException Invalid table exception if the table dictionary returns a null.
     */
    protected LogicalTable generateTable(
            String tableName,
            Granularity granularity,
            LogicalTableDictionary logicalTableDictionary
    ) throws BadApiRequestException {
        return DefaultTableGenerator.generateTable(tableName, granularity, logicalTableDictionary);
    }

    /**
     * Add links to the response builder and return a stream with the requested page from the raw data.
     *
     * @param <T>  The type of the collection elements
     * @param data  The data to be paginated.
     *
     * @return A stream corresponding to the requested page.
     *
     * @deprecated Pagination is moving to a Stream and pushing creation of the page to a more general
     * method ({@link #getPage(Pagination)}) to allow for more flexibility
     * in how pagination is done.
     */
    @Deprecated
    @Override
    public <T> Stream<T> getPage(Collection<T> data) {
        return paginationHelper.getPage(data);
    }

    /**
     * Add links to the response builder and return a stream with the requested page from the raw data.
     *
     * @param <T>  The type of the collection elements
     * @param pagination  The pagination object
     *
     * @return A stream corresponding to the requested page.
     */
    @Override
    public <T> Stream<T> getPage(Pagination<T> pagination) {
        return paginationHelper.getPage(pagination);
    }
}
