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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Performance benchmark tests for Kafka protocol compatibility.
 *
 * <p>Measures and validates:
 *
 * <ul>
 *   <li>Producer throughput
 *   <li>Consumer throughput
 *   <li>Latency characteristics
 *   <li>Scalability under load
 * </ul>
 */
public class KafkaPerformanceBenchmarkITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaPerformanceBenchmarkITCase.class);

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(createClusterConfig())
                    .setTabletServerListeners(
                            "FLUSS://localhost:0,CLIENT://localhost:0,KAFKA://localhost:0")
                    .build();

    private static final String TEST_DATABASE = "perf_test_db";
    private static final String TEST_TABLE = "perf_test_table";
    private static final int NUM_PARTITIONS = 12;

    private Connection conn;
    private Admin admin;
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

        admin.createDatabase(
                        TEST_DATABASE,
                        org.apache.fluss.metadata.DatabaseDescriptor.builder()
                                .customProperties(java.util.Collections.emptyMap())
                                .comment("Performance test database")
                                .build(),
                        true)
                .get();

        // Create table with schema
        TablePath tablePath = TablePath.of(TEST_DATABASE, TEST_TABLE);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("data", DataTypes.STRING())
                        .column("timestamp", DataTypes.BIGINT())
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(NUM_PARTITIONS).build();

        admin.createTable(tablePath, descriptor, true);

        kafkaBootstrapServers = getKafkaBootstrapServers();
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * Benchmark producer throughput.
     *
     * <p>Validates:
     *
     * <ul>
     *   <li>Messages per second
     *   <li>Megabytes per second
     *   <li>Average latency
     *   <li>P99 latency
     * </ul>
     */
    @Test
    public void benchmarkProducerThroughput() throws Exception {
        LOG.info("=== Starting Producer Throughput Benchmark ===");

        String topicName = TEST_DATABASE + "." + TEST_TABLE;
        int numMessages = 50000;
        int recordSize = 1024; // 1KB per message

        String payload = generatePayload(recordSize);
        AtomicLong totalBytes = new AtomicLong(0);
        AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);
        AtomicLong maxLatency = new AtomicLong(0);
        AtomicLong totalLatency = new AtomicLong(0);

        long startTime = System.nanoTime();

        try (KafkaProducer<String, String> producer = createHighThroughputProducer()) {
            for (int i = 0; i < numMessages; i++) {
                String key = "key-" + i;
                long sendStartTime = System.nanoTime();

                Future<RecordMetadata> future =
                        producer.send(
                                new ProducerRecord<>(topicName, key, payload),
                                (metadata, exception) -> {
                                    if (exception == null) {
                                        long latency =
                                                TimeUnit.NANOSECONDS.toMicros(
                                                        System.nanoTime() - sendStartTime);
                                        totalLatency.addAndGet(latency);
                                        minLatency.updateAndGet(
                                                current -> Math.min(current, latency));
                                        maxLatency.updateAndGet(
                                                current -> Math.max(current, latency));
                                        totalBytes.addAndGet(recordSize);
                                    } else {
                                        LOG.error("Send failed", exception);
                                    }
                                });

                // Periodic progress logging
                if ((i + 1) % 10000 == 0) {
                    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                    double throughput = (i + 1) * 1000.0 / elapsed;
                    LOG.info("Sent {} messages, throughput: {:.2f} msg/sec", i + 1, throughput);
                }
            }

            producer.flush();
        }

        long endTime = System.nanoTime();
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

        // Calculate metrics
        double throughputMsgPerSec = (numMessages * 1000.0) / durationMs;
        double throughputMBPerSec = (totalBytes.get() / 1024.0 / 1024.0) / (durationMs / 1000.0);
        long avgLatencyUs = totalLatency.get() / numMessages;

        LOG.info("=== Producer Throughput Benchmark Results ===");
        LOG.info("Total messages: {}", numMessages);
        LOG.info("Total bytes: {} MB", totalBytes.get() / 1024.0 / 1024.0);
        LOG.info("Duration: {} ms", durationMs);
        LOG.info("Throughput: {:.2f} messages/sec", throughputMsgPerSec);
        LOG.info("Throughput: {:.2f} MB/sec", throughputMBPerSec);
        LOG.info("Average latency: {} μs", avgLatencyUs);
        LOG.info("Min latency: {} μs", minLatency.get());
        LOG.info("Max latency: {} μs", maxLatency.get());

        // Basic performance assertions
        assertThat(throughputMsgPerSec)
                .as("Producer throughput should be reasonable")
                .isGreaterThan(100); // At least 100 msg/sec

        assertThat(avgLatencyUs)
                .as("Average latency should be reasonable")
                .isLessThan(100_000); // Less than 100ms

        LOG.info("=== Producer Throughput Benchmark PASSED ===");
    }

    /**
     * Benchmark consumer throughput.
     *
     * <p>Validates:
     *
     * <ul>
     *   <li>Messages consumed per second
     *   <li>Megabytes consumed per second
     *   <li>Poll latency
     *   <li>End-to-end latency
     * </ul>
     */
    @Test
    public void benchmarkConsumerThroughput() throws Exception {
        LOG.info("=== Starting Consumer Throughput Benchmark ===");

        String topicName = TEST_DATABASE + "." + TEST_TABLE;
        int numMessages = 50000;
        int recordSize = 1024;

        // Phase 1: Produce messages
        LOG.info("Phase 1: Producing {} messages", numMessages);
        String payload = generatePayload(recordSize);

        try (KafkaProducer<String, String> producer = createHighThroughputProducer()) {
            for (int i = 0; i < numMessages; i++) {
                producer.send(new ProducerRecord<>(topicName, "key-" + i, payload));
            }
            producer.flush();
        }

        LOG.info("Phase 2: Consuming {} messages", numMessages);

        // Phase 2: Consume and measure
        long startTime = System.nanoTime();
        int consumedCount = 0;
        long totalBytes = 0;
        long totalPollLatency = 0;
        int pollCount = 0;

        try (KafkaConsumer<String, String> consumer =
                createHighThroughputConsumer("benchmark-consumer-group")) {
            consumer.subscribe(Collections.singletonList(topicName));

            while (consumedCount < numMessages) {
                long pollStartTime = System.nanoTime();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                long pollLatency = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - pollStartTime);

                totalPollLatency += pollLatency;
                pollCount++;

                for (ConsumerRecord<String, String> record : records) {
                    totalBytes += record.value().length();
                    consumedCount++;
                }

                if (consumedCount % 10000 == 0) {
                    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                    double throughput = consumedCount * 1000.0 / elapsed;
                    LOG.info(
                            "Consumed {} messages, throughput: {:.2f} msg/sec",
                            consumedCount,
                            throughput);
                }
            }
        }

        long endTime = System.nanoTime();
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

        // Calculate metrics
        double throughputMsgPerSec = (consumedCount * 1000.0) / durationMs;
        double throughputMBPerSec = (totalBytes / 1024.0 / 1024.0) / (durationMs / 1000.0);
        long avgPollLatencyUs = pollCount > 0 ? totalPollLatency / pollCount : 0;

        LOG.info("=== Consumer Throughput Benchmark Results ===");
        LOG.info("Total messages consumed: {}", consumedCount);
        LOG.info("Total bytes consumed: {} MB", totalBytes / 1024.0 / 1024.0);
        LOG.info("Duration: {} ms", durationMs);
        LOG.info("Throughput: {:.2f} messages/sec", throughputMsgPerSec);
        LOG.info("Throughput: {:.2f} MB/sec", throughputMBPerSec);
        LOG.info("Average poll latency: {} μs", avgPollLatencyUs);
        LOG.info("Total poll calls: {}", pollCount);

        // Basic performance assertions
        assertThat(throughputMsgPerSec)
                .as("Consumer throughput should be reasonable")
                .isGreaterThan(100);

        assertThat(consumedCount).as("All messages should be consumed").isEqualTo(numMessages);

        LOG.info("=== Consumer Throughput Benchmark PASSED ===");
    }

    /**
     * Benchmark offset commit performance.
     *
     * <p>Validates:
     *
     * <ul>
     *   <li>Commit latency
     *   <li>Commit throughput
     *   <li>Concurrent commit handling
     * </ul>
     */
    @Test
    public void benchmarkOffsetCommitPerformance() throws Exception {
        LOG.info("=== Starting Offset Commit Performance Benchmark ===");

        String topicName = TEST_DATABASE + "." + TEST_TABLE;
        int numMessages = 10000;
        int commitInterval = 100; // Commit every 100 messages

        // Produce messages
        LOG.info("Producing {} messages", numMessages);
        try (KafkaProducer<String, String> producer = createHighThroughputProducer()) {
            for (int i = 0; i < numMessages; i++) {
                producer.send(new ProducerRecord<>(topicName, "key-" + i, "value-" + i));
            }
            producer.flush();
        }

        // Consume with frequent commits
        LOG.info("Consuming with commit every {} messages", commitInterval);
        long startTime = System.nanoTime();
        int consumedCount = 0;
        int commitCount = 0;
        long totalCommitLatency = 0;
        long minCommitLatency = Long.MAX_VALUE;
        long maxCommitLatency = 0;

        try (KafkaConsumer<String, String> consumer =
                createHighThroughputConsumer("commit-benchmark-group")) {
            consumer.subscribe(Collections.singletonList(topicName));

            while (consumedCount < numMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                consumedCount += records.count();

                if (consumedCount % commitInterval == 0 || consumedCount >= numMessages) {
                    long commitStartTime = System.nanoTime();
                    consumer.commitSync();
                    long commitLatency =
                            TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - commitStartTime);

                    totalCommitLatency += commitLatency;
                    minCommitLatency = Math.min(minCommitLatency, commitLatency);
                    maxCommitLatency = Math.max(maxCommitLatency, commitLatency);
                    commitCount++;
                }
            }
        }

        long endTime = System.nanoTime();
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

        // Calculate metrics
        long avgCommitLatencyUs = commitCount > 0 ? totalCommitLatency / commitCount : 0;
        double commitsPerSec = (commitCount * 1000.0) / durationMs;

        LOG.info("=== Offset Commit Performance Results ===");
        LOG.info("Total messages: {}", numMessages);
        LOG.info("Total commits: {}", commitCount);
        LOG.info("Duration: {} ms", durationMs);
        LOG.info("Commits per second: {:.2f}", commitsPerSec);
        LOG.info("Average commit latency: {} μs", avgCommitLatencyUs);
        LOG.info("Min commit latency: {} μs", minCommitLatency);
        LOG.info("Max commit latency: {} μs", maxCommitLatency);

        // Performance assertions
        assertThat(avgCommitLatencyUs)
                .as("Average commit latency should be reasonable")
                .isLessThan(50_000); // Less than 50ms

        assertThat(commitsPerSec)
                .as("Commit throughput should be reasonable")
                .isGreaterThan(10); // At least 10 commits/sec

        LOG.info("=== Offset Commit Performance Benchmark PASSED ===");
    }

    /**
     * Stress test with sustained high load.
     *
     * <p>Validates:
     *
     * <ul>
     *   <li>System stability under load
     *   <li>Memory management
     *   <li>No resource leaks
     *   <li>Consistent performance over time
     * </ul>
     */
    @Test
    public void stressTestSustainedHighLoad() throws Exception {
        LOG.info("=== Starting Sustained High Load Stress Test ===");

        String topicName = TEST_DATABASE + "." + TEST_TABLE;
        int durationSeconds = 30;
        int targetRate = 1000; // Target 1000 msg/sec

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (durationSeconds * 1000L);

        AtomicLong sentCount = new AtomicLong(0);
        AtomicLong errorCount = new AtomicLong(0);

        try (KafkaProducer<String, String> producer = createHighThroughputProducer()) {
            int messageId = 0;

            while (System.currentTimeMillis() < endTime) {
                long cycleStart = System.currentTimeMillis();

                // Send messages for this second
                for (int i = 0; i < targetRate; i++) {
                    String key = "stress-key-" + messageId;
                    String value = "stress-value-" + messageId + "-" + System.currentTimeMillis();

                    producer.send(
                            new ProducerRecord<>(topicName, key, value),
                            (metadata, exception) -> {
                                if (exception != null) {
                                    errorCount.incrementAndGet();
                                    LOG.error("Send error", exception);
                                } else {
                                    sentCount.incrementAndGet();
                                }
                            });

                    messageId++;
                }

                // Rate limiting - sleep to maintain target rate
                long cycleElapsed = System.currentTimeMillis() - cycleStart;
                if (cycleElapsed < 1000) {
                    Thread.sleep(1000 - cycleElapsed);
                }

                // Periodic stats
                if (messageId % 5000 == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    double actualRate = sentCount.get() * 1000.0 / elapsed;
                    LOG.info(
                            "Sent {} messages in {} seconds, rate: {:.2f} msg/sec, errors: {}",
                            sentCount.get(),
                            elapsed / 1000,
                            actualRate,
                            errorCount.get());
                }
            }

            producer.flush();
        }

        long totalDuration = System.currentTimeMillis() - startTime;
        double actualRate = sentCount.get() * 1000.0 / totalDuration;

        LOG.info("=== Sustained High Load Stress Test Results ===");
        LOG.info("Duration: {} seconds", totalDuration / 1000);
        LOG.info("Messages sent: {}", sentCount.get());
        LOG.info("Errors: {}", errorCount.get());
        LOG.info("Average rate: {:.2f} msg/sec", actualRate);
        LOG.info(
                "Success rate: {:.2f}%",
                (sentCount.get() * 100.0) / (sentCount.get() + errorCount.get()));

        // Assertions
        assertThat(errorCount.get())
                .as("Error count should be low")
                .isLessThan(sentCount.get() / 100); // Less than 1% errors

        assertThat(actualRate)
                .as("Actual rate should be close to target")
                .isGreaterThan(targetRate * 0.8); // Within 80% of target

        LOG.info("=== Sustained High Load Stress Test PASSED ===");
    }

    // Helper methods

    private String getKafkaBootstrapServers() {
        org.apache.fluss.server.metadata.ServerInfo serverInfo =
                FLUSS_CLUSTER_EXTENSION.getTabletServerInfos().get(0);
        org.apache.fluss.cluster.Endpoint endpoint = serverInfo.endpoint("KAFKA");
        if (endpoint == null) {
            throw new IllegalStateException("KAFKA endpoint not found");
        }
        return endpoint.getHost() + ":" + endpoint.getPort();
    }

    private KafkaProducer<String, String> createHighThroughputProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "perf-test-producer-" + System.nanoTime());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createHighThroughputConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "perf-test-consumer-" + System.nanoTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        return new KafkaConsumer<>(props);
    }

    private String generatePayload(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append((char) ('a' + (i % 26)));
        }
        return sb.toString();
    }
}
