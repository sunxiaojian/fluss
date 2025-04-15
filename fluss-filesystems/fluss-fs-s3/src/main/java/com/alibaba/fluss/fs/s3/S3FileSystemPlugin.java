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

package com.alibaba.fluss.fs.s3;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemPlugin;
import com.alibaba.fluss.fs.hdfs.utils.HadoopUtils;
import com.alibaba.fluss.fs.s3.token.S3DelegationTokenReceiver;

import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.IOException;
import java.net.URI;

import static com.alibaba.fluss.fs.s3.token.S3DelegationTokenReceiver.PROVIDER_CONFIG_NAME;

/** Simple factory for the s3 file system. */
public class S3FileSystemPlugin implements FileSystemPlugin {

    private static final String[] FLUSS_CONFIG_PREFIXES = {"s3.", "s3a.", "fs.s3a."};

    private static final String HADOOP_CONFIG_PREFIX = "fs.s3a.";

    private static final String ACCESS_KEY_ID = "fs.s3a.access.key";

    private static final String[][] MIRRORED_CONFIG_KEYS = {
        {"fs.s3a.access-key", "fs.s3a.access.key"},
        {"fs.s3a.secret-key", "fs.s3a.secret.key"},
        {"fs.s3a.path-style-access", "fs.s3a.path.style.access"}
    };

    @Override
    public String getScheme() {
        return "s3";
    }

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                mirrorCertainHadoopConfig(
                        HadoopUtils.createHadoopConfiguration(
                                FLUSS_CONFIG_PREFIXES, HADOOP_CONFIG_PREFIX, flussConfig));

        // set credential provider
        HadoopUtils.setCredentialProvider(
                hadoopConfig, ACCESS_KEY_ID, PROVIDER_CONFIG_NAME, this::updateHadoopConfig);

        // create the Hadoop FileSystem
        org.apache.hadoop.fs.FileSystem fs = S3AFileSystem.get(fsUri, hadoopConfig);
        return new S3FileSystem(getScheme(), fs, hadoopConfig);
    }

    // mirror certain keys to make use more uniform across implementations
    // with different keys
    private org.apache.hadoop.conf.Configuration mirrorCertainHadoopConfig(
            org.apache.hadoop.conf.Configuration hadoopConfig) {
        for (String[] mirrored : MIRRORED_CONFIG_KEYS) {
            String value = hadoopConfig.get(mirrored[0], null);
            if (value != null) {
                hadoopConfig.set(mirrored[1], value);
            }
        }
        return hadoopConfig;
    }

    protected void updateHadoopConfig(org.apache.hadoop.conf.Configuration hadoopConfig) {
        S3DelegationTokenReceiver.updateHadoopConfig(hadoopConfig);
    }
}
