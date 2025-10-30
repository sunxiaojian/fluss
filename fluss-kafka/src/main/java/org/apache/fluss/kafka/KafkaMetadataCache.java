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

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A simple cache for Kafka-to-Fluss metadata mapping.
 *
 * <p>This cache maintains mappings between Kafka topics and Fluss table metadata, reducing the need
 * for repeated metadata lookups.
 */
public class KafkaMetadataCache {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetadataCache.class);

    private final AdminReadOnlyGateway adminGateway;

    // Cache: topic -> table ID
    private final Map<String, Long> topicToTableId = MapUtils.newConcurrentHashMap();

    // Cache: table ID -> topic
    private final Map<Long, String> tableIdToTopic = MapUtils.newConcurrentHashMap();

    // Cache: topic -> TablePath
    private final Map<String, TablePath> topicToTablePath = MapUtils.newConcurrentHashMap();

    public KafkaMetadataCache(AdminReadOnlyGateway adminGateway) {
        this.adminGateway = adminGateway;
    }

    /**
     * Get table ID for the given Kafka topic. Returns null if not found.
     *
     * @param topic Kafka topic name
     * @return Table ID, or null if not cached
     */
    @Nullable
    public Long getTableId(String topic) {
        return topicToTableId.get(topic);
    }

    /**
     * Get topic name for the given table ID. Returns null if not found.
     *
     * @param tableId Fluss table ID
     * @return Kafka topic name, or null if not cached
     */
    @Nullable
    public String getTopic(long tableId) {
        return tableIdToTopic.get(tableId);
    }

    /**
     * Get table path for the given Kafka topic.
     *
     * @param topic Kafka topic name
     * @return TablePath
     */
    public TablePath getOrCreateTablePath(String topic) {
        return topicToTablePath.computeIfAbsent(topic, KafkaProtocolUtils::kafkaTopicToTablePath);
    }

    /**
     * Fetch and cache metadata for the given topic.
     *
     * @param topic Kafka topic name
     * @return CompletableFuture with the table ID, or null if table not found
     */
    public CompletableFuture<Long> fetchAndCacheMetadata(String topic) {
        TablePath tablePath = getOrCreateTablePath(topic);

        MetadataRequest metadataRequest = new MetadataRequest();
        metadataRequest
                .addTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());

        return adminGateway
                .metadata(metadataRequest)
                .thenApply(
                        response -> {
                            if (response.getTableMetadatasCount() > 0) {
                                PbTableMetadata tableMetadata = response.getTableMetadataAt(0);
                                long tableId = tableMetadata.getTableId();

                                // Cache the mappings
                                topicToTableId.put(topic, tableId);
                                tableIdToTopic.put(tableId, topic);

                                LOG.debug(
                                        "Cached metadata for topic {}: tableId={}", topic, tableId);
                                return tableId;
                            } else {
                                LOG.warn("No metadata found for topic: {}", topic);
                                return null;
                            }
                        })
                .exceptionally(
                        throwable -> {
                            LOG.error("Error fetching metadata for topic: " + topic, throwable);
                            return null;
                        });
    }

    /**
     * Get or fetch table ID for the given topic. If not cached, fetches from server.
     *
     * @param topic Kafka topic name
     * @return CompletableFuture with the table ID
     */
    public CompletableFuture<Long> getOrFetchTableId(String topic) {
        Long cachedId = getTableId(topic);
        if (cachedId != null) {
            return CompletableFuture.completedFuture(cachedId);
        }
        return fetchAndCacheMetadata(topic);
    }

    /**
     * Invalidate cached metadata for the given topic.
     *
     * @param topic Kafka topic name
     */
    public void invalidate(String topic) {
        Long tableId = topicToTableId.remove(topic);
        if (tableId != null) {
            tableIdToTopic.remove(tableId);
        }
        topicToTablePath.remove(topic);
    }

    /** Clear all cached metadata. */
    public void clear() {
        topicToTableId.clear();
        tableIdToTopic.clear();
        topicToTablePath.clear();
    }
}
