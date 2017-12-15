// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_GRANULARITY_MISMATCH;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.web.BadApiRequestException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to hold generator code for tables.
 */
public class DefaultTableGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultTableGenerator.class);

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
    public static LogicalTable generateTable(
            String tableName,
            Granularity granularity,
            LogicalTableDictionary logicalTableDictionary
    ) throws BadApiRequestException {
        LogicalTable generated = logicalTableDictionary.get(new TableIdentifier(tableName, granularity));

        // check if requested logical table grain pair exists
        if (generated == null) {
            String msg = TABLE_GRANULARITY_MISMATCH.logFormat(granularity, tableName);
            LOG.error(msg);
            throw new BadApiRequestException(msg);
        }

        LOG.trace("Generated logical table: {} with granularity {}", generated, granularity);
        return generated;
    }
}
