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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.gorpipe.exceptions.GorResourceException;
import org.gorpipe.gor.driver.meta.DataType;
import org.gorpipe.gor.driver.meta.SourceReference;
import org.gorpipe.gor.driver.meta.SourceType;
import org.gorpipe.gor.driver.providers.stream.sources.StreamSource;
import org.gorpipe.gor.driver.providers.stream.sources.StreamSourceMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Represents an object in Amazon S3.
 * Created by villi on 22/08/15.
 */
public class S3ASource implements StreamSource {
    private final SourceReference sourceReference;
    private final Path path;
    private final FileSystem fs;
    private StreamSourceMetadata meta;

    /**
     * Create source
     *
     * @param sourceReference contains S3 url of the form s3://bucket/objectpath
     */
    public S3ASource(SourceReference sourceReference) {
        this(sourceReference, new Path(sourceReference.getUrl()));
    }

    S3ASource(SourceReference sourceReference, Path path) {
        this.sourceReference = sourceReference;
        this.path = path;

        Configuration conf = new Configuration();
        //conf.set("fs.s3a.endpoint","localhost:4566");
        conf.set("fs.s3a.connection.ssl.enabled","false");
        conf.set("fs.s3a.path.style.access","true");
        conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.change.detection.mode","warn");
        conf.set("com.amazonaws.services.s3.enableV4","true");
        conf.set("fs.s3a.committer.name","partitioned");
        conf.set("fs.s3a.committer.staging.conflict-mode","replace");
        conf.set("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.S3SingleDriverLogStore");
        //conf.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");

        try {
            this.fs = path.getFileSystem(conf);
        } catch (IOException e) {
            throw new GorResourceException("","",e);
        }
    }

    @Override
    public InputStream open() throws IOException {
        return fs.open(path);
    }

    @Override
    public InputStream open(long start) throws IOException {
        FSDataInputStream is = fs.open(path);
        is.seek(start);
        return is;
    }

    @Override
    public InputStream open(long start, long minLength) throws IOException {
        FSDataInputStream is = fs.open(path);
        is.seek(start);
        return is;
    }

    @Override
    public String getName() {
        return sourceReference.getUrl();
    }

    @Override
    public StreamSourceMetadata getSourceMetadata() throws IOException {
        if (meta == null) {
            FileStatus fileStatus = fs.getFileStatus(path);
            meta = new StreamSourceMetadata(this, getName(), fileStatus.getModificationTime(), fileStatus.getLen(), null, false);
        }
        return meta;
    }

    @Override
    public OutputStream getOutputStream(boolean append) throws IOException {
        return fs.create(path);
    }

    @Override
    public boolean supportsWriting() {
        return true;
    }

    @Override
    public SourceReference getSourceReference() {
        return sourceReference;
    }

    @Override
    public DataType getDataType() {
        return DataType.fromFileName(path.getName());
    }

    @Override
    public boolean exists() {
        boolean exists = false;
        try {
            exists = fs.exists(path);
        } catch (IOException e) {
            throw new GorResourceException("Hadoop s3 exists failed",path.toString(),e);
        }
        return exists;
    }

    @Override
    public SourceType getSourceType() {
        return S3ASourceType.S3A;
    }

    @Override
    public void close() throws IOException {
        // No resources to free
    }
}
