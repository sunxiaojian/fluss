/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.fs.hdfs;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemPlugin;
import com.alibaba.fluss.fs.UnsupportedFileSystemSchemeException;
import com.alibaba.fluss.fs.hdfs.utils.HadoopUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A file system plugin for Hadoop-based file systems.
 *
 * <p>This plugin calls Hadoop's mechanism to find a file system implementation for a given file
 * system scheme (a {@link org.apache.hadoop.fs.FileSystem}) and wraps it as a Fluss file system (a
 * {@link FileSystem}).
 */
public class HadoopFsPlugin implements FileSystemPlugin {

    public static final String SCHEME = "hdfs";

    /** The prefixes that Fluss adds to the Hadoop config. */
    public static final String[] FLUSS_CONFIG_PREFIXES = {"fluss.hadoop."};

    private static final Logger LOG = LoggerFactory.getLogger(HadoopFsPlugin.class);

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        checkNotNull(fsUri, "fsUri");
        checkArgument(fsUri.getScheme() != null, "file system has null scheme");

        try {
            final org.apache.hadoop.conf.Configuration hadoopConfig =
                    HadoopUtils.getHadoopConfiguration(FLUSS_CONFIG_PREFIXES, flussConfig);
            final org.apache.hadoop.fs.FileSystem hadoopFs =
                    org.apache.hadoop.fs.FileSystem.get(fsUri, hadoopConfig);
            LOG.debug("Created Hadoop FS for {}: {}", fsUri, hadoopFs.getClass().getName());

            return new HadoopFileSystem(hadoopFs);
        } catch (LinkageError e) {
            throw new UnsupportedFileSystemSchemeException(
                    "Cannot support file system for '"
                            + fsUri.getScheme()
                            + "' via Hadoop, because Hadoop is not in the classpath, or some classes "
                            + "are missing from the classpath.",
                    e);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Cannot instantiate file system for URI: " + fsUri, e);
        }
    }
}
