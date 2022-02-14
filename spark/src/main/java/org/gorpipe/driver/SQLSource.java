/*
 *  BEGIN_COPYRIGHT
 *
 *  Copyright (C) 2011-2013 deCODE genetics Inc.
 *  Copyright (C) 2013-2019 WuXi NextCode Inc.
 *  All Rights Reserved.
 *
 *  GORpipe is free software: you can redistribute it and/or modify
 *  it under the terms of the AFFERO GNU General Public License as published by
 *  the Free Software Foundation.
 *
 *  GORpipe is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
 *  INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
 *  NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
 *  the AFFERO GNU General Public License for the complete license terms.
 *
 *  You should have received a copy of the AFFERO GNU General Public License
 *  along with GORpipe.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
 *
 *  END_COPYRIGHT
 */

package org.gorpipe.driver;

import org.apache.spark.sql.SparkSession;
import org.gorpipe.exceptions.GorResourceException;
import org.gorpipe.gor.driver.meta.DataType;
import org.gorpipe.gor.driver.meta.SourceReference;
import org.gorpipe.gor.driver.meta.SourceType;
import org.gorpipe.gor.driver.providers.stream.sources.StreamSource;
import org.gorpipe.gor.driver.providers.stream.sources.StreamSourceMetadata;
import org.gorpipe.spark.GorSparkUtilities;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Represents an table in SQL database
 * Created by villi on 22/08/15.
 */
public class SQLSource implements StreamSource {
    private final SourceReference sourceReference;
    private final String table;
    private StreamSourceMetadata meta;
    private SparkSession spark;

    /**
     * Create source
     *
     * @param sourceReference contains sql url of the form sql://table
     */
    public SQLSource(SourceReference sourceReference) {
        this(sourceReference, sourceReference.getUrl());
    }

    SQLSource(SourceReference sourceReference, String table) {
        this.sourceReference = sourceReference;
        this.table = table.substring(6).toLowerCase();
        this.spark = GorSparkUtilities.getSparkSession();
    }

    @Override
    public InputStream open() {
        return open(0);
    }

    @Override
    public InputStream open(long start) {
        return open(start,0);
    }

    @Override
    public InputStream open(long start, long minLength) {
        throw new GorResourceException("Not implemented", table);
    }

    @Override
    public String getName() {
        return sourceReference.getUrl();
    }

    @Override
    public StreamSourceMetadata getSourceMetadata() {
        if (meta == null) {
            var ds = spark.table(table);
            meta = new StreamSourceMetadata(this, getName(), Instant.now().toEpochMilli(), 0L, "g"+ds.hashCode(), false);
        }
        return meta;
    }

    @Override
    public OutputStream getOutputStream(boolean append) {
        throw new GorResourceException("Not implemented", table);
    }

    @Override
    public boolean supportsWriting() {
        return true;
    }

    @Override
    public boolean supportsLinks() {
        return false;
    }

    @Override
    public SourceReference getSourceReference() {
        return sourceReference;
    }

    @Override
    public DataType getDataType() {
        return DataType.GOR;
    }

    @Override
    public boolean exists() {
        String[] tableNames = spark.sqlContext().tableNames();
        var tableSet = new HashSet<>(Arrays.asList(tableNames));
        return tableSet.contains(table);
    }

    @Override
    public SourceType getSourceType() {
        return SQLSourceType.SQL;
    }

    @Override
    public void close() throws IOException {
        // No resources to free
    }
}
