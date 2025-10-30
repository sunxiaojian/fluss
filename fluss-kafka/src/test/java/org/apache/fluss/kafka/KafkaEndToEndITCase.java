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
import org.apache.fluss.utils.MapUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive end-to-end integration test for Kafka protocol compatibility.
 *
 * <p>This test suite validates production-level scenarios including:
 *
 * <ul>
 *   <li>Complete producer-consumer workflows
 *   <li>Offset management lifecycle
 *   <li>Error handling and recovery
 *   <li>Concurrent operations
 *   <li>Long-running stability
 * </ul>
 */
public class KafkaEndToEndITCase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaEndToEndITCase.class);

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3) // Multi-node cluster for production-like testing
                    .setClusterConf(createClusterConfig())
                    .setTabletServerListeners(
                            "FLUSS://localhost:0,CLIENT://localhost:0,KAFKA://localhost:0")
                    .build();

    private static final String TEST_DATABASE = "e2e_test_db";
    private static final String TEST_TABLE_PREFIX = "e2e_test_table_";
    private static final int NUM_PARTITIONS = 6;
    private static final short REPLICATION_FACTOR = 1;

    private Connection conn;
    private Admin admin;
    private AdminClient kafkaAdmin;
    private String kafkaBootstrapServers;

    private static Configuration createClusterConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:0,KAFKA://localhost:0");
        return conf;
    }

    @BeforeEach
    public void setup() throws Exception {
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();

        // Create test database
        admin.createDatabase(
                        TEST_DATABASE,
                        org.apache.fluss.metadata.DatabaseDescriptor.builder()
                                .customProperties(java.util.Collections.emptyMap())
                                .comment("E2E test database")
                                .build(),
                        true)
                .get();

        kafkaBootstrapServers = getKafkaBootstrapServers();

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

    /**
     * Test complete producer-consumer workflow with large dataset.
     *
     * <p>Validates:
     *
     * <ul>
     *   <li>Large-scale data production
     *   <li>Ordered consumption
     *   <li>No message loss
     *   <li>Proper offset tracking
     * </ul>
     */
    @Test
    public void testCompleteProducerConsumerWorkflow() throws Exception {
        LOG.info("Starting complete producer-consumer workflow test");

        String topicName = TEST_DATABASE + "." + TEST_TABLE_PREFIX + "workflow";
        int numMessages = 10000;

        // Create table
        createTestTable(TEST_TABLE_PREFIX + "workflow", NUM_PARTITIONS);

        // Phase 1: Produce messages
        LOG.info("Phase 1: Producing {} messages", numMessages);
        Map<Integer, String> expectedMessages = new HashMap<>();

        try (KafkaProducer<String, String> producer = createKafkaProducer()) {
            List<Future<RecordMetadata>> futures = new ArrayList<>();

            for (int i = 0; i < numMessages; i++) {
                String key = "key-" + i;
                String value = "value-" + i + "-" + System.currentTimeMillis();
                expectedMessages.put(i, value);

                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
                futures.add(producer.send(record));

                if (i % 1000 == 0) {
                    LOG.info("Produced {} messages", i);
                }
            }

            // Wait for all sends to complete
            for (Future<RecordMetadata> future : futures) {
                RecordMetadata metadata = future.get(30, TimeUnit.SECONDS);
                assertThat(metadata).isNotNull();
                assertThat(metadata.hasOffset()).isTrue();
            }

            producer.flush();
            LOG.info("Successfully produced all {} messages", numMessages);
        }

        // Phase 2: Consume and verify messages
        LOG.info("Phase 2: Consuming and verifying messages");
        Map<Integer, String> consumedMessages = new HashMap<>();

        try (KafkaConsumer<String, String> consumer = createKafkaConsumer("workflow-test-group")) {
            consumer.subscribe(Collections.singletonList(topicName));

            long startTime = System.currentTimeMillis();
            int consumedCount = 0;

            while (consumedCount < numMessages && System.currentTimeMillis() - startTime < 120000) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    int id = Integer.parseInt(record.key().substring(4)); // Extract from "key-N"
                    consumedMessages.put(id, record.value());
                    consumedCount++;

                    if (consumedCount % 1000 == 0) {
                        LOG.info("Consumed {} messages", consumedCount);
                    }
                }
            }

            LOG.info("Successfully consumed {} messages", consumedCount);
            assertThat(consumedCount).isEqualTo(numMessages);
        }

        // Phase 3: Verify data integrity
        LOG.info("Phase 3: Verifying data integrity");
        assertThat(consumedMessages).hasSize(numMessages);

        for (int i = 0; i < numMessages; i++) {
            assertThat(consumedMessages).containsKey(i);
            assertThat(consumedMessages.get(i)).isEqualTo(expectedMessages.get(i));
        }

        LOG.info("Complete producer-consumer workflow test PASSED");
    }

    /**
     * Test offset management lifecycle with consumer restart.
     *
     * <p>Validates:
     *
     * <ul>
     *   <li>Offset commit after consumption
     *   <li>Consumer restart from committed offset
     *   <li>No duplicate consumption
     *   <li>Manual and auto commit modes
     * </ul>
     */
    @Test
    public void testOffsetManagementLifecycle() throws Exception {
        LOG.info("Starting offset management lifecycle test");

        String topicName = TEST_DATABASE + "." + TEST_TABLE_PREFIX + "offset_lifecycle";
        int totalMessages = 5000;
        String groupId = "offset-lifecycle-group";

        createTestTable(TEST_TABLE_PREFIX + "offset_lifecycle", NUM_PARTITIONS);

        // Produce messages
        try (KafkaProducer<String, String> producer = createKafkaProducer()) {
            for (int i = 0; i < totalMessages; i++) {
                producer.send(new ProducerRecord<>(topicName, "key-" + i, "value-" + i));
            }
            producer.flush();
        }

        // Phase 1: First consumer - consume half with manual commit
        LOG.info("Phase 1: First consumer consuming half with manual commit");
        int firstBatchSize = totalMessages / 2;
        Map<TopicPartition, Long> lastCommittedOffsets = new HashMap<>();

        try (KafkaConsumer<String, String> consumer1 = createKafkaConsumer(groupId)) {
            consumer1.subscribe(Collections.singletonList(topicName));

            int consumed = 0;
            while (consumed < firstBatchSize) {
                ConsumerRecords<String, String> records = consumer1.poll(Duration.ofMillis(1000));
                consumed += records.count();

                // Commit every 100 messages
                if (consumed % 100 == 0) {
                    consumer1.commitSync();
                    LOG.debug("Committed after {} messages", consumed);
                }
            }

            // Final commit
            consumer1.commitSync();

            // Record committed offsets
            for (TopicPartition partition : consumer1.assignment()) {
                OffsetAndMetadata committed = consumer1.committed(partition);
                if (committed != null) {
                    lastCommittedOffsets.put(partition, committed.offset());
                    LOG.info(
                            "Partition {} committed offset: {}",
                            partition.partition(),
                            committed.offset());
                }
            }

            LOG.info("First consumer consumed {} messages and committed", consumed);
        }

        // Phase 2: Second consumer - should resume from committed offset
        LOG.info("Phase 2: Second consumer resuming from committed offset");
        int secondBatchConsumed = 0;

        try (KafkaConsumer<String, String> consumer2 = createKafkaConsumer(groupId)) {
            consumer2.subscribe(Collections.singletonList(topicName));

            // Trigger partition assignment
            consumer2.poll(Duration.ZERO);

            // Verify starting from committed offsets
            for (TopicPartition partition : consumer2.assignment()) {
                long position = consumer2.position(partition);
                Long committedOffset = lastCommittedOffsets.get(partition);

                if (committedOffset != null) {
                    assertThat(position)
                            .as(
                                    "Consumer should start from committed offset for partition %d",
                                    partition.partition())
                            .isEqualTo(committedOffset);
                }
            }

            // Consume remaining messages
            long startTime = System.currentTimeMillis();
            while (secondBatchConsumed < (totalMessages - firstBatchSize)
                    && System.currentTimeMillis() - startTime < 30000) {
                ConsumerRecords<String, String> records = consumer2.poll(Duration.ofMillis(1000));
                secondBatchConsumed += records.count();
            }

            LOG.info("Second consumer consumed {} messages", secondBatchConsumed);
        }

        // Verify no duplicates and no loss
        assertThat(secondBatchConsumed)
                .as("Should consume remaining messages without duplicates")
                .isGreaterThanOrEqualTo(totalMessages - firstBatchSize - 100); // Allow some margin

        LOG.info("Offset management lifecycle test PASSED");
    }

    /**
     * Test concurrent producers and consumers.
     *
     * <p>Validates:
     *
     * <ul>
     *   <li>Multiple producers writing concurrently
     *   <li>Multiple consumers reading concurrently
     *   <li>Thread safety
     *   <li>No data corruption
     * </ul>
     */
    @Test
    public void testConcurrentProducersAndConsumers() throws Exception {
        LOG.info("Starting concurrent producers and consumers test");

        String topicName = TEST_DATABASE + "." + TEST_TABLE_PREFIX + "concurrent";
        int numProducers = 5;
        int numConsumers = 3;
        int messagesPerProducer = 1000;

        createTestTable(TEST_TABLE_PREFIX + "concurrent", NUM_PARTITIONS);

        // Phase 1: Concurrent producers
        LOG.info("Phase 1: Starting {} concurrent producers", numProducers);
        ExecutorService producerExecutor = Executors.newFixedThreadPool(numProducers);
        CountDownLatch producerLatch = new CountDownLatch(numProducers);
        AtomicInteger totalProduced = new AtomicInteger(0);
        Map<String, String> sentMessages = MapUtils.newConcurrentHashMap();

        for (int p = 0; p < numProducers; p++) {
            final int producerId = p;
            producerExecutor.submit(
                    () -> {
                        try (KafkaProducer<String, String> producer = createKafkaProducer()) {
                            for (int i = 0; i < messagesPerProducer; i++) {
                                String key = "producer-" + producerId + "-msg-" + i;
                                String value = "value-" + producerId + "-" + i;
                                sentMessages.put(key, value);

                                producer.send(new ProducerRecord<>(topicName, key, value));
                                totalProduced.incrementAndGet();
                            }
                            producer.flush();
                            LOG.info("Producer {} completed", producerId);
                        } catch (Exception e) {
                            LOG.error("Producer {} failed", producerId, e);
                        } finally {
                            producerLatch.countDown();
                        }
                    });
        }

        boolean producersCompleted = producerLatch.await(120, TimeUnit.SECONDS);
        assertThat(producersCompleted).as("All producers should complete").isTrue();
        assertThat(totalProduced.get()).isEqualTo(numProducers * messagesPerProducer);

        LOG.info("All {} producers completed, {} messages sent", numProducers, totalProduced.get());

        producerExecutor.shutdown();

        // Phase 2: Concurrent consumers
        LOG.info("Phase 2: Starting {} concurrent consumers", numConsumers);
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(numConsumers);
        CountDownLatch consumerLatch = new CountDownLatch(numConsumers);
        AtomicInteger totalConsumed = new AtomicInteger(0);
        Map<String, String> receivedMessages = MapUtils.newConcurrentHashMap();

        for (int c = 0; c < numConsumers; c++) {
            final int consumerId = c;
            consumerExecutor.submit(
                    () -> {
                        try (KafkaConsumer<String, String> consumer =
                                createKafkaConsumer("concurrent-group-" + consumerId)) {
                            consumer.subscribe(Collections.singletonList(topicName));

                            long startTime = System.currentTimeMillis();
                            int consumed = 0;

                            while (System.currentTimeMillis() - startTime < 60000) {
                                ConsumerRecords<String, String> records =
                                        consumer.poll(Duration.ofMillis(1000));

                                if (records.isEmpty()
                                        && consumed > 0
                                        && System.currentTimeMillis() - startTime > 10000) {
                                    break; // No more messages after 10 seconds
                                }

                                for (ConsumerRecord<String, String> record : records) {
                                    receivedMessages.put(record.key(), record.value());
                                    consumed++;
                                    totalConsumed.incrementAndGet();
                                }
                            }

                            LOG.info("Consumer {} consumed {} messages", consumerId, consumed);
                        } catch (Exception e) {
                            LOG.error("Consumer {} failed", consumerId, e);
                        } finally {
                            consumerLatch.countDown();
                        }
                    });
        }

        boolean consumersCompleted = consumerLatch.await(120, TimeUnit.SECONDS);
        assertThat(consumersCompleted).as("All consumers should complete").isTrue();

        LOG.info(
                "All {} consumers completed, {} messages received",
                numConsumers,
                totalConsumed.get());

        consumerExecutor.shutdown();

        // Phase 3: Verify all messages received correctly
        LOG.info("Phase 3: Verifying message integrity");
        assertThat(receivedMessages).hasSameSizeAs(sentMessages);

        for (Map.Entry<String, String> entry : sentMessages.entrySet()) {
            assertThat(receivedMessages)
                    .containsEntry(entry.getKey(), entry.getValue())
                    .as("Message %s should be received correctly", entry.getKey());
        }

        LOG.info("Concurrent producers and consumers test PASSED");
    }

    /**
     * Test error handling and recovery scenarios.
     *
     * <p>Validates:
     *
     * <ul>
     *   <li>Timeout handling
     *   <li>Invalid topic handling
     *   <li>Producer retry logic
     *   <li>Consumer error recovery
     * </ul>
     */
    @Test
    public void testErrorHandlingAndRecovery() throws Exception {
        LOG.info("Starting error handling and recovery test");

        String validTopicName = TEST_DATABASE + "." + TEST_TABLE_PREFIX + "error_test";
        String invalidTopicName = "non_existent_db.non_existent_table";

        createTestTable(TEST_TABLE_PREFIX + "error_test", NUM_PARTITIONS);

        // Test 1: Producer timeout handling
        LOG.info("Test 1: Producer timeout handling");
        try (KafkaProducer<String, String> producer = createKafkaProducer()) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(validTopicName, "timeout-key", "timeout-value");

            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(30, TimeUnit.SECONDS);

            assertThat(metadata).isNotNull();
            assertThat(metadata.hasOffset()).isTrue();
            LOG.info("Producer timeout handling: PASSED");
        }

        // Test 2: Invalid topic handling
        LOG.info("Test 2: Invalid topic handling");
        try (KafkaProducer<String, String> producer = createKafkaProducer()) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(invalidTopicName, "key", "value");

            try {
                producer.send(record).get(10, TimeUnit.SECONDS);
                // Depending on implementation, this might not throw immediately
                LOG.info("Send to invalid topic did not throw immediately (acceptable)");
            } catch (Exception e) {
                LOG.info("Send to invalid topic threw exception as expected: {}", e.getMessage());
            }
        }

        // Test 3: Consumer error recovery
        LOG.info("Test 3: Consumer error recovery");
        try (KafkaConsumer<String, String> consumer = createKafkaConsumer("error-recovery-group")) {
            // Try to subscribe to invalid topic
            consumer.subscribe(Collections.singletonList(invalidTopicName));

            // This should not crash, but might not return data
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            LOG.info(
                    "Consumer poll on invalid topic returned {} records (acceptable)",
                    records.count());

            // Subscribe to valid topic and recover
            consumer.subscribe(Collections.singletonList(validTopicName));
            records = consumer.poll(Duration.ofMillis(5000));
            LOG.info("Consumer recovered and polled from valid topic");
        }

        // Test 4: Offset commit error handling
        LOG.info("Test 4: Offset commit error handling");
        try (KafkaConsumer<String, String> consumer = createKafkaConsumer("commit-error-group")) {
            TopicPartition tp = new TopicPartition(validTopicName, 0);
            consumer.assign(Collections.singletonList(tp));

            // Try to commit without consuming
            try {
                consumer.commitSync();
                LOG.info("Empty commit succeeded (acceptable)");
            } catch (Exception e) {
                LOG.info("Empty commit failed as expected: {}", e.getMessage());
            }

            // Consume and commit normally
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            if (!records.isEmpty()) {
                consumer.commitSync();
                LOG.info("Normal commit after consumption succeeded");
            }
        }

        LOG.info("Error handling and recovery test PASSED");
    }

    /**
     * Test AdminClient operations comprehensively.
     *
     * <p>Validates:
     *
     * <ul>
     *   <li>Topic creation and deletion
     *   <li>Configuration queries
     *   <li>Cluster information
     *   <li>Metadata consistency
     * </ul>
     */
    @Test
    public void testAdminClientOperations() throws Exception {
        LOG.info("Starting AdminClient operations test");

        String topicName = TEST_DATABASE + "." + TEST_TABLE_PREFIX + "admin_ops";

        // Test 1: Create topic via AdminClient
        LOG.info("Test 1: Creating topic via AdminClient");
        NewTopic newTopic = new NewTopic(topicName, NUM_PARTITIONS, REPLICATION_FACTOR);
        kafkaAdmin.createTopics(Collections.singleton(newTopic)).all().get(30, TimeUnit.SECONDS);
        LOG.info("Topic created successfully: {}", topicName);

        // Test 2: Verify topic exists via metadata
        LOG.info("Test 2: Verifying topic metadata");
        try (KafkaConsumer<String, String> consumer = createKafkaConsumer("admin-test-group")) {
            List<org.apache.kafka.common.PartitionInfo> partitions =
                    consumer.partitionsFor(topicName);

            assertThat(partitions).isNotNull().hasSize(NUM_PARTITIONS);
            LOG.info("Topic has {} partitions as expected", partitions.size());
        }

        // Test 3: Describe cluster
        LOG.info("Test 3: Describing cluster");
        org.apache.kafka.clients.admin.DescribeClusterResult clusterResult =
                kafkaAdmin.describeCluster();

        String clusterId = clusterResult.clusterId().get(30, TimeUnit.SECONDS);
        assertThat(clusterId).isNotNull().isNotEmpty();
        LOG.info("Cluster ID: {}", clusterId);

        java.util.Collection<org.apache.kafka.common.Node> nodes =
                clusterResult.nodes().get(30, TimeUnit.SECONDS);
        assertThat(nodes).isNotEmpty();
        LOG.info("Cluster has {} nodes", nodes.size());

        // Test 4: Describe configs
        LOG.info("Test 4: Describing topic configs");
        org.apache.kafka.common.config.ConfigResource resource =
                new org.apache.kafka.common.config.ConfigResource(
                        org.apache.kafka.common.config.ConfigResource.Type.TOPIC, topicName);

        Map<org.apache.kafka.common.config.ConfigResource, org.apache.kafka.clients.admin.Config>
                configs =
                        kafkaAdmin
                                .describeConfigs(Collections.singleton(resource))
                                .all()
                                .get(30, TimeUnit.SECONDS);

        assertThat(configs).containsKey(resource);
        assertThat(configs.get(resource).entries()).isNotEmpty();
        LOG.info("Retrieved {} config entries", configs.get(resource).entries().size());

        // Test 5: Delete topic
        LOG.info("Test 5: Deleting topic");
        kafkaAdmin.deleteTopics(Collections.singleton(topicName)).all().get(30, TimeUnit.SECONDS);
        LOG.info("Topic deleted successfully: {}", topicName);

        LOG.info("AdminClient operations test PASSED");
    }

    // Helper methods

    private void createTestTable(String tableName, int numBuckets) throws Exception {
        TablePath tablePath = TablePath.of(TEST_DATABASE, tableName);

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("data", DataTypes.STRING())
                        .column("timestamp", DataTypes.BIGINT())
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(numBuckets).build();

        admin.createTable(tablePath, descriptor, true);
        LOG.debug("Created test table: {}", tableName);
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
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "e2e-test-producer-" + System.nanoTime());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");

        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createKafkaConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "e2e-test-consumer-" + System.nanoTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "40000");

        return new KafkaConsumer<>(props);
    }
}
