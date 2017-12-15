// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.util.AllPagesPagination;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.util.PaginationLink;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

public class PaginationHelper {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final int DEFAULT_PER_PAGE = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("default_per_page")
    );

    private static final int DEFAULT_PAGE = 1;
    private static final PaginationParameters DEFAULT_PAGINATION = new PaginationParameters(
            DEFAULT_PER_PAGE,
            DEFAULT_PAGE
    );

    Pagination pagination;
    private final PaginationParameters paginationParameters;
    private final UriInfo uriInfo;
    private final Response.ResponseBuilder builder;

    public PaginationHelper(UriInfo uriInfo) {
        this(uriInfo, getDefaultPagination(), Response.status(Response.Status.OK));
    }

    public PaginationHelper(UriInfo uriInfo, PaginationParameters paginationParameters) {
        this(uriInfo, paginationParameters, Response.status(Response.Status.OK));
    }

    public PaginationHelper(UriInfo uriInfo, String page, String perPage) {
        this(
                uriInfo,
                DefaultOutputFormatGenerators.generatePaginationParameters(perPage, page),
                Response.status(Response.Status.OK)
        );
    }

    public PaginationHelper(UriInfo uriInfo, String page, String perPage, Response.ResponseBuilder builder) {
        this(
                uriInfo,
                DefaultOutputFormatGenerators.generatePaginationParameters(perPage, page),
                builder
        );
    }
    /**
     * Get the default pagination parameters for this type of API request.
     *
     * @return The uri info of this type of API request
     */
    public static PaginationParameters getDefaultPagination() {
        return DEFAULT_PAGINATION;
    }

    /**
     * This method returns a Function that can basically take a Collection and return an instance of
     * AllPagesPagination.
     *
     * @param paginationParameters  The PaginationParameters to be used to generate AllPagesPagination instance
     * @param <T>  The type of items in the Collection which needs to be paginated
     *
     * @return A Function that takes a Collection and returns an instance of AllPagesPagination
     */
    public static <T> Function<Collection<T>, AllPagesPagination<T>> getAllPagesPaginationFactory(
            PaginationParameters paginationParameters
    ) {
        return data -> new AllPagesPagination<>(data, paginationParameters);
    }

    public PaginationHelper(
            UriInfo uriInfo,
            PaginationParameters paginationParameters,
            Response.ResponseBuilder builder
    ) {
        this.uriInfo = uriInfo;
        this.paginationParameters = paginationParameters == null ? paginationParameters : getDefaultPagination();
        this.builder = builder;
    }

    public PaginationParameters getPaginationParameters() {
        return paginationParameters;
    }

    /**
     * Add page links to the header of the response builder.
     *
     * @param link  The type of the link to add.
     * @param pages  The paginated set of results containing the pages being linked to.
     */
    protected void addPageLink(PaginationLink link, Pagination<?> pages) {
        link.getPage(pages).ifPresent(page -> addPageLink(link, page));
    }

    /**
     * Add page links to the header of the response builder.
     *
     * @param link  The type of the link to add.
     * @param pageNumber  Number of the page to add the link for.
     */
    protected void addPageLink(PaginationLink link, int pageNumber) {
        UriBuilder uriBuilder = uriInfo.getRequestUriBuilder().replaceQueryParam("page", pageNumber);
        builder.header(HttpHeaders.LINK, Link.fromUriBuilder(uriBuilder).rel(link.getHeaderName()).build());
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
    public <T> Stream<T> getPage(Collection<T> data) {
        return getPage(new AllPagesPagination<>(data, paginationParameters));
    }

    /**
     * Add links to the response builder and return a stream with the requested page from the raw data.
     *
     * @param <T>  The type of the collection elements
     * @param pagination  The pagination object
     *
     * @return A stream corresponding to the requested page.
     */
    public <T> Stream<T> getPage(Pagination<T> pagination) {
        this.pagination = pagination;

        Arrays.stream(PaginationLink.values()).forEachOrdered(link -> addPageLink(link, pagination));

        return pagination.getPageOfData().stream();
    }
}
