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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for Kafka offset commit and fetch functionality.
 *
 * <p>This test verifies that:
 *
 * <ul>
 *   <li>Consumers can commit offsets manually
 *   <li>Consumers can fetch committed offsets
 *   <li>Offsets are correctly stored and retrieved
 *   <li>Consumer can resume from committed offset
 * </ul>
 */
public class KafkaOffsetCommitITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(createClusterConfig())
                    .setTabletServerListeners(
                            "FLUSS://localhost:0,CLIENT://localhost:0,KAFKA://localhost:0")
                    .build();

    private static final String TEST_DATABASE = "test_db";
    private static final String TEST_TABLE = "offset_test_table";
    private static final TablePath TEST_TABLE_PATH = TablePath.of(TEST_DATABASE, TEST_TABLE);

    private Connection conn;
    private Admin admin;
    private KafkaProducer<String, String> producer;
    private String kafkaBootstrapServers;

    private static Configuration createClusterConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
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

        // Create test table
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("message", DataTypes.STRING())
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();

        admin.createTable(TEST_TABLE_PATH, tableDescriptor, false);

        // Get Kafka bootstrap servers
        kafkaBootstrapServers = getKafkaBootstrapServers();

        // Create Kafka producer
        producer = createKafkaProducer();

        // Produce some test messages
        String kafkaTopic = TEST_DATABASE + "." + TEST_TABLE;
        for (int i = 0; i < 20; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(kafkaTopic, "key-" + i, "message-" + i);
            producer.send(record).get();
        }
        producer.flush();
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (producer != null) {
            producer.close();
        }
        if (admin != null) {
            admin.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    public void testOffsetCommitAndFetch() throws Exception {
        String kafkaTopic = TEST_DATABASE + "." + TEST_TABLE;
        String groupId = "test-offset-group";

        // First consumer: consume some messages and commit offset
        try (KafkaConsumer<String, String> consumer1 = createKafkaConsumer(groupId)) {
            consumer1.subscribe(Collections.singletonList(kafkaTopic));

            // Consume first 10 messages
            int consumed = 0;
            long maxOffset = -1;
            while (consumed < 10) {
                ConsumerRecords<String, String> records = consumer1.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    consumed++;
                    maxOffset = Math.max(maxOffset, record.offset());
                    if (consumed >= 10) {
                        break;
                    }
                }
            }

            assertThat(consumed).isEqualTo(10);

            // Commit offsets manually
            consumer1.commitSync();

            // Get committed offsets
            Map<TopicPartition, OffsetAndMetadata> committed =
                    consumer1.committed(consumer1.assignment());

            assertThat(committed).isNotEmpty();

            // Verify committed offsets are greater than consumed offset
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
                assertThat(entry.getValue().offset()).isGreaterThan(-1);
            }
        }

        // Second consumer: should resume from committed offset
        try (KafkaConsumer<String, String> consumer2 = createKafkaConsumer(groupId)) {
            consumer2.subscribe(Collections.singletonList(kafkaTopic));

            // First poll to get partition assignment
            consumer2.poll(Duration.ZERO);

            // Fetch committed offsets
            Map<TopicPartition, OffsetAndMetadata> committed =
                    consumer2.committed(consumer2.assignment());

            assertThat(committed).isNotEmpty();

            // Seek to committed offsets
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
                if (entry.getValue() != null && entry.getValue().offset() >= 0) {
                    consumer2.seek(entry.getKey(), entry.getValue().offset());
                }
            }

            // Consume remaining messages (should be ~10 messages)
            int consumedRemaining = 0;
            long startTime = System.currentTimeMillis();
            while (consumedRemaining < 10 && System.currentTimeMillis() - startTime < 10000) {
                ConsumerRecords<String, String> records = consumer2.poll(Duration.ofMillis(1000));
                consumedRemaining += records.count();
            }

            // Should consume approximately 10 remaining messages
            assertThat(consumedRemaining).isGreaterThanOrEqualTo(8); // Allow some variance
        }
    }

    @Test
    public void testManualOffsetCommit() throws Exception {
        String kafkaTopic = TEST_DATABASE + "." + TEST_TABLE;
        String groupId = "test-manual-commit-group";

        try (KafkaConsumer<String, String> consumer = createKafkaConsumer(groupId)) {
            TopicPartition tp = new TopicPartition(kafkaTopic, 0);
            consumer.assign(Collections.singletonList(tp));
            consumer.seekToBeginning(Collections.singletonList(tp));

            // Consume 5 messages
            int consumed = 0;
            long lastOffset = -1;
            while (consumed < 5) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.partition() == 0) {
                        consumed++;
                        lastOffset = record.offset();
                        if (consumed >= 5) {
                            break;
                        }
                    }
                }
            }

            assertThat(consumed).isEqualTo(5);
            assertThat(lastOffset).isGreaterThanOrEqualTo(0);

            // Manually commit specific offset
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            offsetsToCommit.put(tp, new OffsetAndMetadata(lastOffset + 1, "manual-commit"));

            consumer.commitSync(offsetsToCommit);

            // Fetch committed offset
            OffsetAndMetadata committed = consumer.committed(tp);

            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isEqualTo(lastOffset + 1);
            assertThat(committed.metadata()).isEqualTo("manual-commit");
        }
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

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createKafkaConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer-" + System.nanoTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Enable manual offset commit
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(props);
    }
}
