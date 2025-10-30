/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for Kafka CREATE_TOPICS and DELETE_TOPICS support.
 *
 * <p>This test verifies that:
 *
 * <ul>
 *   <li>Kafka Admin Client can create topics (tables) in Fluss
 *   <li>Kafka Admin Client can delete topics (tables) from Fluss
 *   <li>Kafka Admin Client can list topics from Fluss
 * </ul>
 */
public class KafkaCreateDeleteTopicsITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(createClusterConfig())
                    .setTabletServerListeners(
                            "FLUSS://localhost:0,CLIENT://localhost:0,KAFKA://localhost:0")
                    .build();

    private static final String TEST_DATABASE = "test_db";

    private Connection conn;
    private Admin admin;
    private AdminClient kafkaAdmin;
    private String kafkaBootstrapServers;

    private static Configuration createClusterConfig() {
        Configuration conf = new Configuration();
        // Enable Kafka protocol support
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        // Set up listeners for both Fluss and Kafka protocols
        conf.setString(ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:0,KAFKA://localhost:0");
        return conf;
    }

    @BeforeEach
    public void setup() throws Exception {
        // Create connection to Fluss
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();

        // Create test database
        admin.createDatabase(
                        TEST_DATABASE,
                        org.apache.fluss.metadata.DatabaseDescriptor.builder()
                                .customProperties(java.util.Collections.emptyMap())
                                .comment("Test database for Kafka topic operations")
                                .build(),
                        false)
                .get();

        // Get Kafka bootstrap servers
        kafkaBootstrapServers = getKafkaBootstrapServers();

        // Create Kafka Admin client
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        kafkaAdmin = AdminClient.create(adminProps);
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (kafkaAdmin != null) {
            kafkaAdmin.close();
        }
        if (admin != null) {
            admin.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    public void testCreateTopic() throws Exception {
        String topicName = TEST_DATABASE + ".test_create_topic";

        // Create topic via Kafka Admin Client
        NewTopic newTopic = new NewTopic(topicName, 3, (short) 1);
        CreateTopicsResult createResult = kafkaAdmin.createTopics(Collections.singleton(newTopic));
        createResult.all().get();

        // Verify topic exists in Fluss
        TablePath tablePath = TablePath.of(TEST_DATABASE, "test_create_topic");
        boolean exists = admin.tableExists(tablePath).get();
        assertThat(exists).isTrue();
    }

    @Test
    public void testDeleteTopic() throws Exception {
        String topicName = TEST_DATABASE + ".test_delete_topic";
        TablePath tablePath = TablePath.of(TEST_DATABASE, "test_delete_topic");

        // First create a topic
        NewTopic newTopic = new NewTopic(topicName, 2, (short) 1);
        kafkaAdmin.createTopics(Collections.singleton(newTopic)).all().get();

        // Verify it exists
        assertThat(admin.tableExists(tablePath).get()).isTrue();

        // Delete topic via Kafka Admin Client
        DeleteTopicsResult deleteResult = kafkaAdmin.deleteTopics(Collections.singleton(topicName));
        deleteResult.all().get();

        // Verify topic is deleted in Fluss
        assertThat(admin.tableExists(tablePath).get()).isFalse();
    }

    @Test
    public void testListTopics() throws Exception {
        // Create a topic first
        String topicName = TEST_DATABASE + ".test_list_topic";
        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
        kafkaAdmin.createTopics(Collections.singleton(newTopic)).all().get();

        // List topics
        ListTopicsResult listResult = kafkaAdmin.listTopics();
        Set<String> topicNames = listResult.names().get();

        // Verify our topic is in the list
        assertThat(topicNames).contains(topicName);
    }

    private String getKafkaBootstrapServers() {
        // Get the Kafka listener endpoint from the Fluss tablet server
        org.apache.fluss.server.metadata.ServerInfo serverInfo =
                FLUSS_CLUSTER_EXTENSION.getTabletServerInfos().get(0);
        org.apache.fluss.cluster.Endpoint endpoint = serverInfo.endpoint("KAFKA");
        if (endpoint == null) {
            throw new IllegalStateException("KAFKA endpoint not found on tablet server");
        }
        return endpoint.getHost() + ":" + endpoint.getPort();
    }
}
