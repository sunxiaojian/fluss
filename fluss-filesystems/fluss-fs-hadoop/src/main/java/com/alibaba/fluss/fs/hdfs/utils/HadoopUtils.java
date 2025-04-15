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

package com.alibaba.fluss.fs.hdfs.utils;

import com.alibaba.fluss.config.ConfigBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Utility class for working with Hadoop-related classes. This should only be used if Hadoop is on
 * the classpath.
 */
public class HadoopUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);

    static final Text HDFS_DELEGATION_TOKEN_KIND = new Text("HDFS_DELEGATION_TOKEN");

    public static Configuration getHadoopConfiguration(
            String[] prefixes, com.alibaba.fluss.config.Configuration flussConfiguration) {

        // Instantiate an HdfsConfiguration to load the hdfs-site.xml and hdfs-default.xml
        // from the classpath

        Configuration result = new HdfsConfiguration();

        // We need to load both core-site.xml and hdfs-site.xml to determine the default fs path and
        // the hdfs configuration.
        // The properties of a newly added resource will override the ones in previous resources, so
        // a configuration
        // file with higher priority should be added later.

        // Approach 1: HADOOP_HOME environment variables
        boolean foundHadoopConfiguration = loadHadoopConfigurationFromEnv(result);

        // Approach 2: Fluss configuration
        // add all configuration key with prefix 'fluss.hadoop.' in fluss conf to hadoop conf
        Configuration extractConfiguration =
                createHadoopConfiguration(prefixes, flussConfiguration);
        if (extractConfiguration.iterator().hasNext()) {
            result.addResource(extractConfiguration);
            foundHadoopConfiguration = true;
        }

        if (!foundHadoopConfiguration) {
            LOG.warn(
                    "Could not find Hadoop configuration via any of the supported methods "
                            + "(Fluss configuration, environment variables).");
        }
        return result;
    }

    private static boolean loadHadoopConfigurationFromEnv(Configuration result) {
        boolean foundHadoopConfiguration = false;
        String[] possibleHadoopConfPaths = new String[2];

        final String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome != null) {
            LOG.debug("Searching Hadoop configuration files in HADOOP_HOME: {}", hadoopHome);
            possibleHadoopConfPaths[0] = hadoopHome + "/conf";
            possibleHadoopConfPaths[1] = hadoopHome + "/etc/hadoop"; // hadoop 2.2
        }

        for (String possibleHadoopConfPath : possibleHadoopConfPaths) {
            if (possibleHadoopConfPath != null) {
                foundHadoopConfiguration = addHadoopConfIfFound(result, possibleHadoopConfPath);
            }
        }

        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        if (hadoopConfDir != null) {
            LOG.debug("Searching Hadoop configuration files in HADOOP_CONF_DIR: {}", hadoopConfDir);
            foundHadoopConfiguration =
                    addHadoopConfIfFound(result, hadoopConfDir) || foundHadoopConfiguration;
        }
        return foundHadoopConfiguration;
    }

    public static Configuration createHadoopConfiguration(
            String[] prefixes, com.alibaba.fluss.config.Configuration flussConfiguration) {
        return createHadoopConfiguration(prefixes, null, flussConfiguration);
    }

    public static Configuration createHadoopConfiguration(
            String[] prefixes,
            String replacePrefix,
            com.alibaba.fluss.config.Configuration flussConfiguration) {
        Configuration configuration = new Configuration();
        boolean replacePrefixNotEmpty = StringUtils.isNotEmpty(replacePrefix);
        Arrays.stream(prefixes)
                .distinct()
                .sorted((a, b) -> Integer.compare(b.length(), a.length()))
                .forEach(
                        prefix ->
                                flussConfiguration.keySet().stream()
                                        .filter(key -> key.startsWith(prefix))
                                        .forEach(
                                                key -> {
                                                    String newKey =
                                                            replacePrefixNotEmpty
                                                                    ? replacePrefix
                                                                            + key.substring(
                                                                                    prefix.length())
                                                                    : key.substring(
                                                                            prefix.length());
                                                    configuration.set(
                                                            newKey,
                                                            flussConfiguration.getString(
                                                                    ConfigBuilder.key(key)
                                                                            .stringType()
                                                                            .noDefaultValue(),
                                                                    null));
                                                }));
        return configuration;
    }

    public static void setCredentialProvider(
            org.apache.hadoop.conf.Configuration hadoopConfig,
            String accessKeyIdKey,
            String credentialsProviderKey,
            Consumer<Configuration> credentialUpdater) {
        if (hadoopConfig.get(accessKeyIdKey) == null) {
            LOG.info(
                    "{} is not set, using credential provider {}.",
                    accessKeyIdKey,
                    hadoopConfig.get(credentialsProviderKey));
            credentialUpdater.accept(hadoopConfig);
        } else {
            LOG.info("{} is set, using provided credentials.", accessKeyIdKey);
        }
    }

    public static boolean isKerberosSecurityEnabled(UserGroupInformation ugi) {
        return UserGroupInformation.isSecurityEnabled()
                && ugi.getAuthenticationMethod()
                        == UserGroupInformation.AuthenticationMethod.KERBEROS;
    }

    public static boolean areKerberosCredentialsValid(
            UserGroupInformation ugi, boolean useTicketCache) {
        checkState(isKerberosSecurityEnabled(ugi));

        // note: UGI::hasKerberosCredentials inaccurately reports false
        // for logins based on a keytab (fixed in Hadoop 2.6.1, see HADOOP-10786),
        // so we check only in ticket cache scenario.
        if (useTicketCache && !ugi.hasKerberosCredentials()) {
            if (hasHDFSDelegationToken(ugi)) {
                LOG.warn(
                        "Hadoop security is enabled but current login user does not have Kerberos credentials, "
                                + "use delegation token instead. Fluss application will terminate after token expires.");
                return true;
            } else {
                LOG.error(
                        "Hadoop security is enabled, but current login user has neither Kerberos credentials "
                                + "nor delegation tokens!");
                return false;
            }
        }

        return true;
    }

    /** Indicates whether the user has an HDFS delegation token. */
    public static boolean hasHDFSDelegationToken(UserGroupInformation ugi) {
        Collection<Token<? extends TokenIdentifier>> usrTok = ugi.getTokens();
        for (Token<? extends TokenIdentifier> token : usrTok) {
            if (token.getKind().equals(HDFS_DELEGATION_TOKEN_KIND)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Search Hadoop configuration files in the given path, and add them to the configuration if
     * found.
     */
    private static boolean addHadoopConfIfFound(
            Configuration configuration, String possibleHadoopConfPath) {
        boolean foundHadoopConfiguration = false;
        if (new File(possibleHadoopConfPath).exists()) {
            if (new File(possibleHadoopConfPath + "/core-site.xml").exists()) {
                configuration.addResource(
                        new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/core-site.xml"));
                LOG.debug(
                        String.format(
                                "Adding %s/core-site.xml to hadoop configuration",
                                possibleHadoopConfPath));
                foundHadoopConfiguration = true;
            }
            if (new File(possibleHadoopConfPath + "/hdfs-site.xml").exists()) {
                configuration.addResource(
                        new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/hdfs-site.xml"));
                LOG.debug(
                        "Adding "
                                + possibleHadoopConfPath
                                + "/hdfs-site.xml to hadoop configuration");
                foundHadoopConfiguration = true;
            }
        }
        return foundHadoopConfiguration;
    }
}
