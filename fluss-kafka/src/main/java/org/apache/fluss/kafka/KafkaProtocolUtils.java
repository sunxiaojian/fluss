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

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;

import org.apache.kafka.common.record.MemoryRecords;

import java.nio.ByteBuffer;

/** Utility class for converting between Kafka and Fluss protocol objects. */
public class KafkaProtocolUtils {

    /**
     * Convert Kafka topic name to Fluss table path. Kafka topic names are in the format
     * "database_name.table_name" or just "table_name" (which will use default database).
     */
    public static TablePath kafkaTopicToTablePath(String topic) {
        String[] parts = topic.split("\\.", 2);
        if (parts.length == 2) {
            return new TablePath(parts[0], parts[1]);
        } else {
            // Use default database for simple topic names
            return new TablePath("default_database", topic);
        }
    }

    /** Convert Fluss table path to Kafka topic name. The format is "database_name.table_name". */
    public static String tablePathToKafkaTopic(TablePath tablePath) {
        return tablePath.getDatabaseName() + "." + tablePath.getTableName();
    }

    /**
     * Convert Fluss physical table path (with partition) to Kafka topic name. If the table is
     * partitioned, the format is "database_name.table_name_partition_name".
     */
    public static String physicalTablePathToKafkaTopic(PhysicalTablePath physicalTablePath) {
        TablePath tablePath = physicalTablePath.getTablePath();
        String baseTopic = tablePathToKafkaTopic(tablePath);
        if (physicalTablePath.getPartitionName() != null) {
            return baseTopic + "_" + physicalTablePath.getPartitionName();
        }
        return baseTopic;
    }

    /**
     * Convert Kafka partition ID to Fluss bucket ID. In Kafka, partition is similar to Fluss bucket
     * concept.
     */
    public static int kafkaPartitionToBucket(int partition) {
        return partition;
    }

    /** Convert Fluss bucket ID to Kafka partition ID. */
    public static int bucketToKafkaPartition(int bucket) {
        return bucket;
    }

    /**
     * Create a TableBucket from Kafka topic and partition.
     *
     * @param topic Kafka topic name
     * @param partition Kafka partition ID
     * @param tableId Fluss table ID (must be obtained from metadata)
     * @return TableBucket instance
     */
    public static TableBucket createTableBucket(String topic, int partition, long tableId) {
        return new TableBucket(tableId, kafkaPartitionToBucket(partition));
    }

    /**
     * Create a TableBucket with partition ID for partitioned tables.
     *
     * @param topic Kafka topic name
     * @param partition Kafka partition ID
     * @param tableId Fluss table ID
     * @param partitionId Fluss partition ID (for partitioned tables)
     * @return TableBucket instance with partition ID
     */
    public static TableBucket createTableBucket(
            String topic, int partition, long tableId, Long partitionId) {
        if (partitionId != null) {
            return new TableBucket(tableId, partitionId, kafkaPartitionToBucket(partition));
        }
        return createTableBucket(topic, partition, tableId);
    }

    /**
     * Convert Kafka MemoryRecords to Fluss MemoryLogRecords.
     *
     * <p>Note: This is a shallow conversion that wraps the underlying ByteBuffer. The actual record
     * format conversion happens when the records are written to/read from Fluss storage.
     */
    public static MemoryLogRecords kafkaRecordsToFlussRecords(MemoryRecords kafkaRecords) {
        ByteBuffer buffer = kafkaRecords.buffer().duplicate();
        return MemoryLogRecords.pointToByteBuffer(buffer);
    }

    /**
     * Convert Kafka MemoryRecords to Fluss BytesView for RPC.
     *
     * <p>This wraps the Kafka records as a BytesView that can be used in Fluss RPC messages.
     */
    public static BytesView kafkaRecordsToBytesView(MemoryRecords kafkaRecords) {
        MemoryLogRecords flussRecords = kafkaRecordsToFlussRecords(kafkaRecords);
        return new MemorySegmentBytesView(
                flussRecords.getMemorySegment(),
                flussRecords.getPosition(),
                flussRecords.sizeInBytes());
    }

    /**
     * Convert Fluss MemoryLogRecords to Kafka MemoryRecords.
     *
     * <p>Note: This is a shallow conversion that wraps the underlying memory. The actual record
     * format conversion happens when records are read from Fluss storage.
     */
    public static MemoryRecords flussRecordsToKafkaRecords(MemoryLogRecords flussRecords) {
        ByteBuffer buffer =
                flussRecords
                        .getMemorySegment()
                        .wrap(flussRecords.getPosition(), flussRecords.sizeInBytes());
        return MemoryRecords.readableRecords(buffer);
    }

    /**
     * Convert Kafka offset to Fluss offset. Kafka and Fluss both use sequential offsets, so this is
     * a direct mapping.
     */
    public static long kafkaOffsetToFlussOffset(long kafkaOffset) {
        return kafkaOffset;
    }

    /** Convert Fluss offset to Kafka offset. */
    public static long flussOffsetToKafkaOffset(long flussOffset) {
        return flussOffset;
    }

    /** Validate that a topic name can be converted to a valid Fluss table path. */
    public static boolean isValidTopicName(String topic) {
        if (topic == null || topic.isEmpty()) {
            return false;
        }
        // Basic validation - Fluss table names should not contain special characters
        // except dot (.) for database.table separation
        return topic.matches("[a-zA-Z0-9_\\.]+");
    }
}
