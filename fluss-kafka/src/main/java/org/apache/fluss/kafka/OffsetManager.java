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

import org.apache.fluss.utils.MapUtils;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Manager for consumer group offsets.
 *
 * <p>This is a simple in-memory implementation. Offsets are lost on restart. For production use,
 * offsets should be persisted to a Fluss table (similar to Kafka's __consumer_offsets topic).
 *
 * <p>Thread-safe implementation using ConcurrentHashMap.
 */
public class OffsetManager {

    private static final Logger LOG = LoggerFactory.getLogger(OffsetManager.class);

    // Key: GroupTopicPartition, Value: OffsetAndMetadata
    private final Map<GroupTopicPartition, OffsetAndMetadata> offsets;

    public OffsetManager() {
        this.offsets = MapUtils.newConcurrentHashMap();
    }

    /**
     * Commit offset for a consumer group.
     *
     * @param groupId the consumer group ID
     * @param topic the topic name
     * @param partition the partition index
     * @param offset the offset to commit
     * @param metadata optional metadata string
     */
    public void commitOffset(
            String groupId, String topic, int partition, long offset, String metadata) {
        GroupTopicPartition key = new GroupTopicPartition(groupId, topic, partition);
        OffsetAndMetadata value =
                new OffsetAndMetadata(offset, metadata, System.currentTimeMillis());

        offsets.put(key, value);

        LOG.debug(
                "Committed offset for group={}, topic={}, partition={}, offset={}, metadata={}",
                groupId,
                topic,
                partition,
                offset,
                metadata);
    }

    /**
     * Fetch offset for a consumer group.
     *
     * @param groupId the consumer group ID
     * @param topic the topic name
     * @param partition the partition index
     * @return the offset and metadata, or null if not found
     */
    public OffsetAndMetadata fetchOffset(String groupId, String topic, int partition) {
        GroupTopicPartition key = new GroupTopicPartition(groupId, topic, partition);
        return offsets.get(key);
    }

    /**
     * Fetch offsets for multiple partitions.
     *
     * @param groupId the consumer group ID
     * @param topicPartitions the list of topic partitions
     * @return map of topic partition to offset and metadata
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchOffsets(
            String groupId, Iterable<TopicPartition> topicPartitions) {
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();

        for (TopicPartition tp : topicPartitions) {
            OffsetAndMetadata offsetAndMetadata = fetchOffset(groupId, tp.topic(), tp.partition());
            if (offsetAndMetadata != null) {
                result.put(tp, offsetAndMetadata);
            }
        }

        return result;
    }

    /**
     * Delete all offsets for a consumer group.
     *
     * @param groupId the consumer group ID
     */
    public void deleteGroup(String groupId) {
        offsets.keySet().removeIf(key -> key.groupId.equals(groupId));
        LOG.info("Deleted all offsets for group: {}", groupId);
    }

    /**
     * Get all offsets for a consumer group.
     *
     * @param groupId the consumer group ID
     * @return map of topic partition to offset and metadata
     */
    public Map<TopicPartition, OffsetAndMetadata> getAllOffsetsForGroup(String groupId) {
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();

        offsets.forEach(
                (key, value) -> {
                    if (key.groupId.equals(groupId)) {
                        result.put(new TopicPartition(key.topic, key.partition), value);
                    }
                });

        return result;
    }

    /** Key for offset storage: (groupId, topic, partition). */
    static class GroupTopicPartition {
        final String groupId;
        final String topic;
        final int partition;

        GroupTopicPartition(String groupId, String topic, int partition) {
            this.groupId = groupId;
            this.topic = topic;
            this.partition = partition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GroupTopicPartition that = (GroupTopicPartition) o;
            return partition == that.partition
                    && Objects.equals(groupId, that.groupId)
                    && Objects.equals(topic, that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupId, topic, partition);
        }

        @Override
        public String toString() {
            return "GroupTopicPartition{"
                    + "groupId='"
                    + groupId
                    + '\''
                    + ", topic='"
                    + topic
                    + '\''
                    + ", partition="
                    + partition
                    + '}';
        }
    }

    /** Offset and metadata stored for a partition. */
    public static class OffsetAndMetadata {
        private final long offset;
        private final String metadata;
        private final long commitTimestamp;

        public OffsetAndMetadata(long offset, String metadata, long commitTimestamp) {
            this.offset = offset;
            this.metadata = metadata;
            this.commitTimestamp = commitTimestamp;
        }

        public long offset() {
            return offset;
        }

        public String metadata() {
            return metadata;
        }

        public long commitTimestamp() {
            return commitTimestamp;
        }

        @Override
        public String toString() {
            return "OffsetAndMetadata{"
                    + "offset="
                    + offset
                    + ", metadata='"
                    + metadata
                    + '\''
                    + ", commitTimestamp="
                    + commitTimestamp
                    + '}';
        }
    }
}
