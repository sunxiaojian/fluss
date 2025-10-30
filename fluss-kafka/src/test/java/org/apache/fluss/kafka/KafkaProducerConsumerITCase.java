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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test for Kafka Producer and Consumer compatibility with Fluss.
 *
 * <p>This test verifies that:
 *
 * <ul>
 *   <li>Kafka producers can send messages to Fluss
 *   <li>Kafka consumers can consume messages from Fluss
 *   <li>The protocol conversion between Kafka and Fluss works correctly
 * </ul>
 */
public class KafkaProducerConsumerITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(createClusterConfig())
                    .setTabletServerListeners(
                            "FLUSS://localhost:0,CLIENT://localhost:0,KAFKA://localhost:0")
                    .build();

    private static final String TEST_DATABASE = "test_db";
    private static final String TEST_TABLE = "test_table";
    private static final TablePath TEST_TABLE_PATH = TablePath.of(TEST_DATABASE, TEST_TABLE);

    private Connection conn;
    private Admin admin;
    private AdminClient kafkaAdmin;
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
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
                                .comment("Test database")
                                .build(),
                        false)
                .get();

        // Create test table in Fluss
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("value", DataTypes.STRING())
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3) // 3 buckets
                        .build();

        admin.createTable(TEST_TABLE_PATH, tableDescriptor, false);

        // Get Kafka bootstrap servers from Fluss cluster
        kafkaBootstrapServers = getKafkaBootstrapServers();

        // Create Kafka Admin client
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        kafkaAdmin = AdminClient.create(adminProps);

        // Initialize Kafka producer
        producer = createKafkaProducer();

        // Initialize Kafka consumer
        consumer = createKafkaConsumer();
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
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
    public void testProduceAndConsumeMessages() throws Exception {
        // Kafka topic name should match Fluss table path
        String kafkaTopic = TEST_DATABASE + "." + TEST_TABLE;

        // Subscribe consumer to the topic
        consumer.subscribe(Collections.singletonList(kafkaTopic));

        // Produce messages to Fluss via Kafka producer
        int numMessages = 10;
        for (int i = 0; i < numMessages; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(kafkaTopic, "key-" + i, "value-" + i);

            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();

            assertThat(metadata).isNotNull();
            assertThat(metadata.topic()).isEqualTo(kafkaTopic);
            assertThat(metadata.hasOffset()).isTrue();
        }

        producer.flush();

        // Consume messages from Fluss via Kafka consumer
        int totalConsumed = 0;
        long startTime = System.currentTimeMillis();
        while (totalConsumed < numMessages && System.currentTimeMillis() - startTime < 30000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                assertThat(record.topic()).isEqualTo(kafkaTopic);
                assertThat(record.key()).startsWith("key-");
                assertThat(record.value()).startsWith("value-");
                totalConsumed++;
            }
        }

        assertThat(totalConsumed).isEqualTo(numMessages);
    }

    @Test
    public void testProduceToMultiplePartitions() throws Exception {
        String kafkaTopic = TEST_DATABASE + "." + TEST_TABLE;

        // Produce messages to different partitions
        int numMessages = 30;
        for (int i = 0; i < numMessages; i++) {
            // Distribute messages across partitions (0, 1, 2)
            int partition = i % 3;
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(kafkaTopic, partition, "key-" + i, "value-" + i);

            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();

            assertThat(metadata.partition()).isEqualTo(partition);
            assertThat(metadata.hasOffset()).isTrue();
        }

        producer.flush();
    }

    @Test
    public void testMetadataRequest() throws Exception {
        // This test verifies that Kafka clients can retrieve metadata from Fluss
        String kafkaTopic = TEST_DATABASE + "." + TEST_TABLE;

        // The producer initialization already triggers metadata requests
        // Just verify that we can produce successfully, which means metadata works
        ProducerRecord<String, String> record =
                new ProducerRecord<>(kafkaTopic, "test-key", "test-value");

        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get();

        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(kafkaTopic);
    }

    private String getKafkaBootstrapServers() {
        // Get the Kafka listener endpoint from the Fluss tablet server
        // Find the first tablet server and get its KAFKA endpoint
        org.apache.fluss.server.metadata.ServerInfo serverInfo =
                FLUSS_CLUSTER_EXTENSION.getTabletServerInfos().get(0);
        org.apache.fluss.cluster.Endpoint endpoint = serverInfo.endpoint("KAFKA");
        if (endpoint == null) {
            throw new IllegalStateException("KAFKA endpoint not found on tablet server");
        }
        return endpoint.getHost() + ":" + endpoint.getPort();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "fluss-kafka-test-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fluss-kafka-test-consumer-group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "fluss-kafka-test-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        return new KafkaConsumer<>(props);
    }
}
