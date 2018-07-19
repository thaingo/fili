// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INTEGER_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_UNDEFINED;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TOP_N_UNSORTED;

import com.yahoo.bard.webservice.data.DruidHavingBuilder;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.filterbuilders.DruidFilterBuilder;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.druid.model.having.Having;
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.web.ApiHaving;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.filters.ApiFilters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Data API Request Implementation binds, validates, and models the parts of a request to the data endpoint.
 */
public class DataApiRequestImpl extends ApiRequestImpl implements DataApiRequest {
    private static final Logger LOG = LoggerFactory.getLogger(DataApiRequestImpl.class);
    private final LogicalTable table;

    private final Granularity granularity;

    private final Set<Dimension> dimensions;
    private final LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields;
    private final Set<LogicalMetric> logicalMetrics;
    private final Set<Interval> intervals;
    private final ApiFilters apiFilters;
    private final Map<LogicalMetric, Set<ApiHaving>> havings;
    private final Having having;
    private final LinkedHashSet<OrderByColumn> sorts;
    private final int count;
    private final int topN;

    private final DateTimeZone timeZone;

    private final HavingGenerator havingApiGenerator;

    private final DruidFilterBuilder filterBuilder;

    private final Optional<OrderByColumn> dateTimeSort;


    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  string time granularity in URL
     * @param dimensions  single dimension or multiple dimensions separated by '/' in URL
     * @param logicalMetrics  URL logical metric query string in the format:
     * <pre>{@code single metric or multiple logical metrics separated by ',' }</pre>
     * @param intervals  URL intervals query string in the format:
     * <pre>{@code single interval in ISO 8601 format, multiple values separated by ',' }</pre>
     * @param apiFilters  URL filter query String in the format:
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param havings  URL having query String in the format:<pre>
     * {@code
     * ((metric name)-(operation)((values bounded by [])))(followed by , or end of string)
     * }</pre>
     * @param sorts  string of sort columns along with sort direction in the format:<pre>
     * {@code (metricName or dimensionName)|(sortDirection) eg: pageViews|asc }</pre>
     * @param count  count of number of records to be returned in the response
     * @param topN  number of first records per time bucket to be returned in the response
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param timeZoneId  a joda time zone id
     * @param asyncAfter  How long the user is willing to wait for a synchronous request in milliseconds
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param containerRequest  The container request
     * @param bardConfigResources  The configuration resources used to build this api request
     *
     * @throws BadApiRequestException in the following scenarios:
     * <ol>
     *     <li>Null or empty table name in the API request.</li>
     *     <li>Invalid time grain in the API request.</li>
     *     <li>Invalid dimension in the API request.</li>
     *     <li>Invalid logical metric in the API request.</li>
     *     <li>Invalid interval in the API request.</li>
     *     <li>Invalid filter syntax in the API request.</li>
     *     <li>Invalid filter dimensions in the API request.</li>
     *     <li>Invalid having syntax in the API request.</li>
     *     <li>Invalid having metrics in the API request.</li>
     *     <li>Pagination parameters in the API request that are not positive integers.</li>
     * </ol>
     */
    public DataApiRequestImpl(
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
            ContainerRequestContext containerRequest,
            BardConfigResources bardConfigResources
    ) throws BadApiRequestException {
        this(
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
                timeZoneId,
                asyncAfter,
                perPage,
                page,
                containerRequest,
                bardConfigResources.getDimensionDictionary(),
                bardConfigResources.getMetricDictionary().getScope(Collections.singletonList(tableName)),
                bardConfigResources.getLogicalTableDictionary(),
                bardConfigResources.getSystemTimeZone(),
                bardConfigResources.getGranularityParser(),
                bardConfigResources.getFilterBuilder(),
                bardConfigResources.getHavingApiGenerator()
        );
    }
    /**
     * Parses the API request URL and generates the Api Request object.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  string time granularity in URL
     * @param dimensions  single dimension or multiple dimensions separated by '/' in URL
     * @param logicalMetrics  URL logical metric query string in the format:
     * <pre>{@code single metric or multiple logical metrics separated by ',' }</pre>
     * @param intervals  URL intervals query string in the format:
     * <pre>{@code single interval in ISO 8601 format, multiple values separated by ',' }</pre>
     * @param apiFilters  URL filter query String in the format:
     * <pre>{@code
     * ((field name and operation):((multiple values bounded by [])or(single value))))(followed by , or end of string)
     * }</pre>
     * @param havings  URL having query String in the format:<pre>
     * {@code
     * ((metric name)-(operation)((values bounded by [])))(followed by , or end of string)
     * }</pre>
     * @param sorts  string of sort columns along with sort direction in the format:<pre>
     * {@code (metricName or dimensionName)|(sortDirection) eg: pageViews|asc }</pre>
     * @param count  count of number of records to be returned in the response
     * @param topN  number of first records per time bucket to be returned in the response
     * @param format  response data format JSON or CSV. Default is JSON.
     * @param timeZoneId  a joda time zone id
     * @param asyncAfter  How long the user is willing to wait for a synchronous request in milliseconds
     * @param perPage  number of rows to display per page of results. If present in the original request,
     * must be a positive integer. If not present, must be the empty string.
     * @param page  desired page of results. If present in the original request, must be a positive
     * integer. If not present, must be the empty string.
     * @param containerRequest  The container request
     * @param dimensionDictionary  The dimension dictionary for binding dimensions
     * @param metricDictionary The metric dictionary for binding metrics
     * @param logicalTableDictionary The table dictionary for binding logical tables
     * @param systemTimeZone The default time zone for the system
     * @param granularityParser A tool to process granularities
     * @param druidFilterBuilder A function to build druid filters from Api Filters
     * @param havingGenerator A function to create havings
     *
     * @throws BadApiRequestException in the following scenarios:
     * <ol>
     *     <li>Null or empty table name in the API request.</li>
     *     <li>Invalid time grain in the API request.</li>
     *     <li>Invalid dimension in the API request.</li>
     *     <li>Invalid logical metric in the API request.</li>
     *     <li>Invalid interval in the API request.</li>
     *     <li>Invalid filter syntax in the API request.</li>
     *     <li>Invalid filter dimensions in the API request.</li>
     *     <li>Invalid having syntax in the API request.</li>
     *     <li>Invalid having metrics in the API request.</li>
     *     <li>Pagination parameters in the API request that are not positive integers.</li>
     * </ol>
     */
    public DataApiRequestImpl(
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
            ContainerRequestContext containerRequest,
            DimensionDictionary dimensionDictionary,
            MetricDictionary metricDictionary,
            LogicalTableDictionary logicalTableDictionary,
            DateTimeZone systemTimeZone,
            GranularityParser granularityParser,
            DruidFilterBuilder druidFilterBuilder,
            HavingGenerator havingGenerator
    ) throws BadApiRequestException {
        super(format, asyncAfter, perPage, page, containerRequest.getUriInfo());

        timeZone = DateAndTimeGenerators.generateTimeZone(timeZoneId, systemTimeZone);

        // Time grain must be from allowed interval keywords
        this.granularity = generateGranularity(granularity, timeZone, granularityParser);

        TableIdentifier tableId = new TableIdentifier(tableName, this.granularity);

        // Logical table must be in the logical table dictionary
        this.table = logicalTableDictionary.get(tableId);
        if (this.table == null) {
            LOG.debug(TABLE_UNDEFINED.logFormat(tableName));
            throw new BadApiRequestException(TABLE_UNDEFINED.format(tableName));
        }

        DateTimeFormatter dateTimeFormatter = DateAndTimeGenerators.generateDateTimeFormatter(timeZone);

        this.intervals = DateAndTimeGenerators.generateIntervals(intervals, this.granularity, dateTimeFormatter);

        this.havingApiGenerator = havingGenerator;

        // At least one logical metric is required
        this.logicalMetrics = generateLogicalMetrics(logicalMetrics, metricDictionary, dimensionDictionary, table);
        ApiRequestValidators.validateMetrics(this.logicalMetrics, this.table);

        // Zero or more grouping dimensions may be specified
        this.dimensions = generateDimensions(dimensions, dimensionDictionary);
        ApiRequestValidators.validateRequestDimensions(this.dimensions, this.table);

        // Map of dimension to its fields specified using show clause (matrix params)
        this.perDimensionFields = generateDimensionFields(dimensions, dimensionDictionary);

        // Zero or more filtering dimensions may be referenced
        this.apiFilters = generateFilters(apiFilters, table, dimensionDictionary);
        ApiRequestValidators.validateRequestDimensions(this.apiFilters.keySet(), this.table);

        // Zero or more having queries may be referenced
        this.havings = havingApiGenerator.apply(havings, this.logicalMetrics);

        this.having = DruidHavingBuilder.buildHavings(this.havings);

        //Using the LinkedHashMap to preserve the sort order
        LinkedHashMap<String, SortDirection> sortColumnDirection = generateSortColumns(sorts);

        //Requested sort on dateTime column
        this.dateTimeSort = generateDateTimeSortColumn(sortColumnDirection);

        // Requested sort on metrics - optional, can be empty Set
        this.sorts = generateSortColumns(
                removeDateTimeSortColumn(sortColumnDirection),
                this.logicalMetrics, metricDictionary
        );

        // Overall requested number of rows in the response. Ignores grouping in time buckets.
        this.count = generateInteger(count, "count");

        // This is the validation part for count that is inlined here because currently it is very brief.
        if (this.count < 0) {
            LOG.debug(INTEGER_INVALID.logFormat(count, "count"));
            throw new BadApiRequestException(INTEGER_INVALID.logFormat(count, "count"));
        }

        // Requested number of rows per time bucket in the response
        this.topN = generateInteger(topN, "topN");

        // This is the validation part for topN that is inlined here because currently it is very brief.
        if (this.topN < 0) {
            LOG.debug(INTEGER_INVALID.logFormat(topN, "topN"));
            throw new BadApiRequestException(INTEGER_INVALID.logFormat(topN, "topN"));
        } else if (this.topN > 0 && this.sorts.isEmpty()) {
            LOG.debug(TOP_N_UNSORTED.logFormat(topN));
            throw new BadApiRequestException(TOP_N_UNSORTED.format(topN));
        }

        this.filterBuilder = druidFilterBuilder;

        LOG.debug(
                "Api request: TimeGrain: {}," +
                        " Table: {}," +
                        " Dimensions: {}," +
                        " Dimension Fields: {}," +
                        " Filters: {},\n" +
                        " Havings: {},\n" +
                        " Logical metrics: {},\n\n" +
                        " Sorts: {}," +
                        " Count: {}," +
                        " TopN: {}," +
                        " AsyncAfter: {}" +
                        " Format: {}" +
                        " Pagination: {}",
                this.granularity,
                this.table.getName(),
                this.dimensions,
                this.perDimensionFields,
                this.apiFilters,
                this.havings,
                this.logicalMetrics,
                this.sorts,
                this.count,
                this.topN,
                this.asyncAfter,
                this.format,
                this.paginationHelper.getPaginationParameters()
        );

        ApiRequestValidators.validateAggregatability(this.dimensions, this.apiFilters);
        ApiRequestValidators.validateTimeAlignment(this.granularity, this.intervals);
    }

    /**
     * All argument constructor, meant to be used for rewriting apiRequest.
     *
     * @param format  Format for the response
     * @param paginationParameters  Pagination info
     * @param uriInfo  The URI info
     * @param builder  A response builder
     * @param table  Logical table requested
     * @param granularity  Granularity of the request
     * @param dimensions  Grouping dimensions of the request
     * @param perDimensionFields  Fields for each of the grouped dimensions
     * @param logicalMetrics  Metrics requested
     * @param intervals  Intervals requested
     * @param apiFilters  Global filters
     * @param havings  Top-level Having caluses for the request
     * @param having  Single global Druid Having
     * @param sorts  Sorting info for the request
     * @param count  Global limit for the request
     * @param topN  Count of per-bucket limit (TopN) for the request
     * @param asyncAfter  How long in milliseconds the user is willing to wait for a synchronous response
     * @param timeZone  TimeZone for the request
     * @param havingApiGenerator  A generator to generate havings map for the request
     * @param dateTimeSort  A dateTime sort column with its direction
     * @param filterBuilder A function to build druid filters
     */
    protected DataApiRequestImpl(
            ResponseFormatType format,
            PaginationParameters paginationParameters,
            UriInfo uriInfo,
            Response.ResponseBuilder builder,
            LogicalTable table,
            Granularity granularity,
            Set<Dimension> dimensions,
            LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields,
            Set<LogicalMetric> logicalMetrics,
            Set<Interval> intervals,
            ApiFilters apiFilters,
            Map<LogicalMetric, Set<ApiHaving>> havings,
            Having having,
            LinkedHashSet<OrderByColumn> sorts,
            int count,
            int topN,
            long asyncAfter,
            DateTimeZone timeZone,
            HavingGenerator havingApiGenerator,
            Optional<OrderByColumn> dateTimeSort,
            DruidFilterBuilder filterBuilder
    ) {
        super(format, asyncAfter, paginationParameters, uriInfo, builder);
        this.table = table;
        this.granularity = granularity;
        this.dimensions = dimensions;
        this.perDimensionFields = perDimensionFields;
        this.logicalMetrics = logicalMetrics;
        this.intervals = intervals;
        this.apiFilters = apiFilters;
        this.havings = havings;
        this.having = having;
        this.sorts = sorts;
        this.count = count;
        this.topN = topN;
        this.timeZone = timeZone;
        this.havingApiGenerator = havingApiGenerator;
        this.dateTimeSort = dateTimeSort;
        this.filterBuilder = filterBuilder;
    }

    /**
     * To check whether dateTime column request is first one in the sort list or not.
     *
     * @param sortColumns  LinkedHashMap of columns and its direction. Using LinkedHashMap to preserve the order
     *
     * @return True if dateTime column is first one in the sort list. False otherwise
     */
    protected Boolean isDateTimeFirstSortField(LinkedHashMap<String, SortDirection> sortColumns) {
        if (sortColumns != null) {
            List<String> columns = new ArrayList<>(sortColumns.keySet());
            return columns.get(0).equals(DATE_TIME_STRING);
        } else {
            return false;
        }
    }

    /**
     * Method to remove the dateTime column from map of columns and its direction.
     *
     * @param sortColumns  map of columns and its direction
     *
     * @return  Map of columns and its direction without dateTime sort column
     */
    protected Map<String, SortDirection> removeDateTimeSortColumn(Map<String, SortDirection> sortColumns) {
        if (sortColumns != null && sortColumns.containsKey(DATE_TIME_STRING)) {
            sortColumns.remove(DATE_TIME_STRING);
            return sortColumns;
        } else {
            return sortColumns;
        }
    }

    /**
     * Method to generate DateTime sort column from the map of columns and its direction.
     *
     * @param sortColumns  LinkedHashMap of columns and its direction. Using LinkedHashMap to preserve the order
     *
     * @return Instance of OrderByColumn for dateTime
     */
    protected Optional<OrderByColumn> generateDateTimeSortColumn(LinkedHashMap<String, SortDirection> sortColumns) {
        return DefaultSortColumnGenerators.generateDateTimeSortColumn(sortColumns);
    }

    /**
     * Method to convert sort list to column and direction map.
     *
     * @param sorts  String of sort columns
     *
     * @return LinkedHashMap of columns and their direction. Using LinkedHashMap to preserve the order
     */
    protected LinkedHashMap<String, SortDirection> generateSortColumns(String sorts) {
        return DefaultSortColumnGenerators.generateSortColumns(sorts);
    }

    /**
     * Extracts the list of dimensions from the url dimension path segments and "show" matrix params and generates a map
     * of dimension to dimension fields which needs to be annotated on the response.
     * <p>
     * If no "show" matrix param has been set, it returns the default dimension fields configured for the dimension.
     *
     * @param apiDimensionPathSegments  Path segments for the dimensions
     * @param dimensionDictionary  Dimension dictionary to look the dimensions up in
     *
     * @return A map of dimension to requested dimension fields
     */
    protected LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> generateDimensionFields(
            @NotNull List<PathSegment> apiDimensionPathSegments,
            @NotNull DimensionDictionary dimensionDictionary
    ) {
        return DefaultDimensionGenerators.generateDimensionFields(apiDimensionPathSegments, dimensionDictionary);
    }

    /**
     * Extracts the list of metrics from the url metric query string and generates a set of LogicalMetrics.
     *
     * @param apiMetricQuery  URL query string containing the metrics separated by ','.
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects.
     * @param dimensionDictionary  Dimension dictionary to look the dimension up in
     * @param table  The logical table for the data request
     *
     * @return set of metric objects
     * @throws BadApiRequestException if the metric dictionary returns a null or if the apiMetricQuery is invalid.
     */
    protected LinkedHashSet<LogicalMetric> generateLogicalMetrics(
            String apiMetricQuery,
            MetricDictionary metricDictionary,
            DimensionDictionary dimensionDictionary,
            LogicalTable table
    ) throws BadApiRequestException {
        return DefaultLogicalMetricsGenerators.generateLogicalMetrics(
                apiMetricQuery,
                metricDictionary,
                dimensionDictionary,
                table,
                filterBuilder
        );
    }

    /**
     * Generate current date based on granularity.
     *
     * @param dateTime  The current moment as a DateTime
     * @param timeGrain  The time grain used to round the date time
     *
     * @return truncated current date based on granularity
     */
    protected DateTime getCurrentDate(DateTime dateTime, TimeGrain timeGrain) {
        return timeGrain.roundFloor(dateTime);
    }

    /**
     * Generates a Set of OrderByColumn.
     *
     * @param sortDirectionMap  Map of columns and their direction
     * @param logicalMetrics  Set of LogicalMetrics in the query
     * @param metricDictionary  Metric dictionary contains the map of valid metric names and logical metric objects.
     *
     * @return a Set of OrderByColumn
     * @throws BadApiRequestException if the sort clause is invalid.
     */
    protected LinkedHashSet<OrderByColumn> generateSortColumns(
            Map<String, SortDirection> sortDirectionMap,
            Set<LogicalMetric> logicalMetrics,
            MetricDictionary metricDictionary
    ) throws BadApiRequestException {

        return DefaultSortColumnGenerators.generateSortColumns(sortDirectionMap, logicalMetrics, metricDictionary);
    }

    /**
     * Parses the requested input String by converting it to an integer, while treating null as zero.
     *
     * @param value  The requested integer value as String.
     * @param parameterName  The parameter name that corresponds to the requested integer value.
     *
     * @return The integer corresponding to {@code value} or zero if {@code value} is null.
     * @throws BadApiRequestException if the input String can not be parsed as an integer.
     */
    protected int generateInteger(String value, String parameterName) throws BadApiRequestException {
        try {
            return value == null ? 0 : Integer.parseInt(value);
        } catch (NumberFormatException nfe) {
            LOG.debug(INTEGER_INVALID.logFormat(value, parameterName), nfe);
            throw new BadApiRequestException(INTEGER_INVALID.logFormat(value, parameterName), nfe);
        }
    }

    /**
     * Gets the filter dimensions form the given set of filter objects.
     *
     * @return Set of filter dimensions.
     */
    @Override
    public Set<Dimension> getFilterDimensions() {
        return apiFilters.keySet();
    }

    @Override
    public LogicalTable getTable() {
        return this.table;
    }

    @Override
    public Granularity getGranularity() {
        return this.granularity;
    }

    @Override
    public Set<Dimension> getDimensions() {
        return this.dimensions;
    }

    @Override
    public LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> getDimensionFields() {
        return this.perDimensionFields;
    }

    @Override
    public Set<LogicalMetric> getLogicalMetrics() {
        return this.logicalMetrics;
    }

    @Override
    public Set<Interval> getIntervals() {
        return this.intervals;
    }

    @Override
    public ApiFilters getApiFilters() {
        return this.apiFilters;
    }

    @Override
    public Map<LogicalMetric, Set<ApiHaving>> getHavings() {
        return this.havings;
    }

    @Override
    public Having getHaving() {
        return this.having;
    }

    @Override
    public LinkedHashSet<OrderByColumn> getSorts() {
        return this.sorts;
    }

    @Override
    public OptionalInt getCount() {
        return count == 0 ? OptionalInt.empty() : OptionalInt.of(count);
    }

    @Override
    public OptionalInt getTopN() {
        return topN == 0 ? OptionalInt.empty() : OptionalInt.of(topN);
    }

    @Override
    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    @Override
    public Optional<OrderByColumn> getDateTimeSort() {
        return dateTimeSort;
    }

    @Override
    public DruidFilterBuilder getFilterBuilder() {
        return filterBuilder;
    }

    // CHECKSTYLE:OFF
    @Override
    public DataApiRequest withFormat(ResponseFormatType format) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withPaginationParameters(Optional<PaginationParameters> paginationParameters) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withUriInfo(UriInfo uriInfo) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withTable(LogicalTable table) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withGranularity(Granularity granularity) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withDimensions(Set<Dimension> dimensions) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withPerDimensionFields(LinkedHashMap<Dimension, LinkedHashSet<DimensionField>> perDimensionFields) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withLogicalMetrics(Set<LogicalMetric> logicalMetrics) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withIntervals(Set<Interval> intervals) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withFilters(ApiFilters apiFilters) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withHavings(Map<LogicalMetric, Set<ApiHaving>> havings) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withHaving(Having having) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withSorts(LinkedHashSet<OrderByColumn> sorts) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withCount(int count) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withTopN(int topN) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withAsyncAfter(long asyncAfter) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withTimeZone(DateTimeZone timeZone) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    @Override
    public DataApiRequest withFilterBuilder(DruidFilterBuilder filterBuilder) {
        return new DataApiRequestImpl(format, getPaginationParameters(), uriInfo, builder, table, granularity, dimensions, perDimensionFields, logicalMetrics, intervals, apiFilters, havings, having, sorts, count, topN, asyncAfter, timeZone, havingApiGenerator, dateTimeSort, filterBuilder);
    }

    // CHECKSTYLE:ON
}
