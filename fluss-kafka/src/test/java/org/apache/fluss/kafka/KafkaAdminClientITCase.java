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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for Kafka AdminClient functionality.
 *
 * <p>This test verifies that:
 *
 * <ul>
 *   <li>AdminClient can describe cluster
 *   <li>AdminClient can describe configs
 *   <li>AdminClient can find coordinator
 *   <li>AdminClient can create/delete topics
 * </ul>
 */
public class KafkaAdminClientITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(createClusterConfig())
                    .setTabletServerListeners(
                            "FLUSS://localhost:0,CLIENT://localhost:0,KAFKA://localhost:0")
                    .build();

    private AdminClient kafkaAdmin;
    private String kafkaBootstrapServers;

    private static Configuration createClusterConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:0,KAFKA://localhost:0");
        return conf;
    }

    @BeforeEach
    public void setup() {
        kafkaBootstrapServers = getKafkaBootstrapServers();

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");

        kafkaAdmin = AdminClient.create(adminProps);
    }

    @AfterEach
    public void cleanup() {
        if (kafkaAdmin != null) {
            kafkaAdmin.close();
        }
    }

    @Test
    public void testDescribeCluster() throws ExecutionException, InterruptedException {
        DescribeClusterResult result = kafkaAdmin.describeCluster();

        // Get cluster ID
        String clusterId = result.clusterId().get();
        assertThat(clusterId).isNotNull().isNotEmpty();

        // Get nodes (brokers)
        Collection<Node> nodes = result.nodes().get();
        assertThat(nodes).isNotEmpty();

        for (Node node : nodes) {
            assertThat(node.id()).isGreaterThanOrEqualTo(0);
            assertThat(node.host()).isNotNull();
            assertThat(node.port()).isGreaterThan(0);
        }

        // Get controller
        Node controller = result.controller().get();
        assertThat(controller).isNotNull();
        assertThat(controller.id()).isGreaterThanOrEqualTo(0);
    }

    @Test
    public void testDescribeTopicConfigs() throws Exception {
        // Create a topic first
        String topicName = "test_db.config_test_topic";
        NewTopic newTopic = new NewTopic(topicName, 3, (short) 1);
        kafkaAdmin.createTopics(Collections.singleton(newTopic)).all().get();

        // Describe topic configs
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        DescribeConfigsResult describeResult =
                kafkaAdmin.describeConfigs(Collections.singleton(resource));

        Map<ConfigResource, Config> configs = describeResult.all().get();

        assertThat(configs).isNotEmpty();
        assertThat(configs).containsKey(resource);

        Config topicConfig = configs.get(resource);
        assertThat(topicConfig).isNotNull();
        assertThat(topicConfig.entries()).isNotEmpty();

        // Check for some common Kafka configs
        boolean hasRetentionConfig =
                topicConfig.entries().stream()
                        .anyMatch(entry -> entry.name().equals("retention.ms"));
        boolean hasSegmentConfig =
                topicConfig.entries().stream()
                        .anyMatch(entry -> entry.name().equals("segment.bytes"));

        assertThat(hasRetentionConfig || hasSegmentConfig).isTrue();

        // Cleanup
        kafkaAdmin.deleteTopics(Collections.singleton(topicName)).all().get();
    }

    @Test
    public void testDescribeBrokerConfigs() throws Exception {
        // Get first broker ID
        DescribeClusterResult clusterResult = kafkaAdmin.describeCluster();
        Collection<Node> nodes = clusterResult.nodes().get();
        assertThat(nodes).isNotEmpty();

        int brokerId = nodes.iterator().next().id();

        // Describe broker configs
        ConfigResource resource =
                new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
        DescribeConfigsResult describeResult =
                kafkaAdmin.describeConfigs(Collections.singleton(resource));

        Map<ConfigResource, Config> configs = describeResult.all().get();

        assertThat(configs).isNotEmpty();
        assertThat(configs).containsKey(resource);

        Config brokerConfig = configs.get(resource);
        assertThat(brokerConfig).isNotNull();
        assertThat(brokerConfig.entries()).isNotEmpty();
    }

    @Test
    public void testFindCoordinator() throws Exception {
        // Find coordinator for a consumer group
        String groupId = "test-consumer-group";

        Node coordinator =
                kafkaAdmin
                        .describeConsumerGroups(Collections.singleton(groupId))
                        .all()
                        .get()
                        .get(groupId)
                        .coordinator();

        // Note: For consumer groups, we might get null if group doesn't exist yet
        // But the API should not throw an exception
    }

    @Test
    public void testCreateAndDescribeTopic() throws Exception {
        String topicName = "test_db.admin_test_topic";

        // Create topic
        NewTopic newTopic = new NewTopic(topicName, 3, (short) 1);
        kafkaAdmin.createTopics(Collections.singleton(newTopic)).all().get();

        // Describe topic using metadata
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = createConsumer();
        try {
            java.util.List<org.apache.kafka.common.PartitionInfo> partitions =
                    consumer.partitionsFor(topicName);

            assertThat(partitions).isNotNull();
            assertThat(partitions.size()).isEqualTo(3); // 3 partitions

            // Verify partition info
            for (org.apache.kafka.common.PartitionInfo partition : partitions) {
                assertThat(partition.topic()).isEqualTo(topicName);
                assertThat(partition.partition()).isGreaterThanOrEqualTo(0).isLessThan(3);
                assertThat(partition.leader()).isNotNull();
            }
        } finally {
            consumer.close();
        }

        // Delete topic
        kafkaAdmin.deleteTopics(Collections.singleton(topicName)).all().get();
    }

    private String getKafkaBootstrapServers() {
        org.apache.fluss.server.metadata.ServerInfo serverInfo =
                FLUSS_CLUSTER_EXTENSION.getTabletServerInfos().get(0);
        org.apache.fluss.cluster.Endpoint endpoint = serverInfo.endpoint("KAFKA");
        if (endpoint == null) {
            throw new IllegalStateException("KAFKA endpoint not found");
        }
        return endpoint.getHost() + ":" + endpoint.getPort();
    }

    private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(
                org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaBootstrapServers);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "admin-test");
        props.put(
                org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        props.put(
                org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        props.put(
                org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "false");

        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    }
}
