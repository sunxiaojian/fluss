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

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CreateTableRequest;
import org.apache.fluss.rpc.messages.DropTableRequest;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbFetchLogReqForTable;
import org.apache.fluss.rpc.messages.PbFetchLogRespForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogRespForTable;
import org.apache.fluss.rpc.messages.PbListOffsetsRespForBucket;
import org.apache.fluss.rpc.messages.PbProduceLogReqForBucket;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.netty.server.RequestHandler;
import org.apache.fluss.rpc.protocol.RequestType;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/** Kafka protocol implementation for request handler. */
public class KafkaRequestHandler implements RequestHandler<KafkaRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRequestHandler.class);

    // TODO: we may need a new abstraction between TabletService and ReplicaManager to avoid
    //  affecting Fluss protocol when supporting compatibility with Kafka.
    private final TabletServerGateway gateway;
    private final AdminReadOnlyGateway adminGateway;
    private final KafkaMetadataCache metadataCache;
    private final OffsetManager offsetManager;

    public KafkaRequestHandler(TabletServerGateway gateway) {
        this.gateway = gateway;
        // TabletServerGateway extends AdminReadOnlyGateway
        this.adminGateway =
                gateway instanceof AdminReadOnlyGateway ? (AdminReadOnlyGateway) gateway : null;
        this.metadataCache = adminGateway != null ? new KafkaMetadataCache(adminGateway) : null;
        this.offsetManager = new OffsetManager();
    }

    /**
     * Check if gateway supports admin write operations (create/delete tables). TabletService
     * provides these via delegation to Coordinator.
     */
    private boolean supportsAdminWriteOps() {
        // Check if gateway is TabletService (which has coordinatorGateway delegation)
        return gateway.getClass().getName().contains("TabletService");
    }

    @Override
    public RequestType requestType() {
        return RequestType.KAFKA;
    }

    @Override
    public void processRequest(KafkaRequest request) {
        // See kafka.server.KafkaApis#handle
        switch (request.apiKey()) {
            case API_VERSIONS:
                handleApiVersionsRequest(request);
                break;
            case METADATA:
                handleMetadataRequest(request);
                break;
            case PRODUCE:
                handleProducerRequest(request);
                break;
            case FIND_COORDINATOR:
                handleFindCoordinatorRequest(request);
                break;
            case LIST_OFFSETS:
                handleListOffsetRequest(request);
                break;
            case OFFSET_FETCH:
                handleOffsetFetchRequest(request);
                break;
            case OFFSET_COMMIT:
                handleOffsetCommitRequest(request);
                break;
            case FETCH:
                handleFetchRequest(request);
                break;
            case JOIN_GROUP:
                handleJoinGroupRequest(request);
                break;
            case SYNC_GROUP:
                handleSyncGroupRequest(request);
                break;
            case HEARTBEAT:
                handleHeartbeatRequest(request);
                break;
            case LEAVE_GROUP:
                handleLeaveGroupRequest(request);
                break;
            case DESCRIBE_GROUPS:
                handleDescribeGroupsRequest(request);
                break;
            case LIST_GROUPS:
                handleListGroupsRequest(request);
                break;
            case DELETE_GROUPS:
                handleDeleteGroupsRequest(request);
                break;
            case SASL_HANDSHAKE:
                handleSaslHandshakeRequest(request);
                break;
            case SASL_AUTHENTICATE:
                handleSaslAuthenticateRequest(request);
                break;
            case CREATE_TOPICS:
                handleCreateTopicsRequest(request);
                break;
            case INIT_PRODUCER_ID:
                handleInitProducerIdRequest(request);
                break;
            case ADD_PARTITIONS_TO_TXN:
                handleAddPartitionsToTxnRequest(request);
                break;
            case ADD_OFFSETS_TO_TXN:
                handleAddOffsetsToTxnRequest(request);
                break;
            case TXN_OFFSET_COMMIT:
                handleTxnOffsetCommitRequest(request);
                break;
            case END_TXN:
                handleEndTxnRequest(request);
                break;
            case WRITE_TXN_MARKERS:
                handleWriteTxnMarkersRequest(request);
                break;
            case DESCRIBE_CONFIGS:
                handleDescribeConfigsRequest(request);
                break;
            case ALTER_CONFIGS:
                handleAlterConfigsRequest(request);
                break;
            case DELETE_TOPICS:
                handleDeleteTopicsRequest(request);
                break;
            case DELETE_RECORDS:
                handleDeleteRecordsRequest(request);
                break;
            case OFFSET_DELETE:
                handleOffsetDeleteRequest(request);
                break;
            case CREATE_PARTITIONS:
                handleCreatePartitionsRequest(request);
                break;
            case DESCRIBE_CLUSTER:
                handleDescribeClusterRequest(request);
                break;
            default:
                handleUnsupportedRequest(request);
        }
    }

    private void handleUnsupportedRequest(KafkaRequest request) {
        String message = String.format("Unsupported request with api key %s", request.apiKey());
        AbstractRequest abstractRequest = request.request();
        AbstractResponse response =
                abstractRequest.getErrorResponse(new UnsupportedOperationException(message));
        request.complete(response);
    }

    void handleApiVersionsRequest(KafkaRequest request) {
        short apiVersion = request.apiVersion();
        if (!ApiKeys.API_VERSIONS.isVersionSupported(apiVersion)) {
            request.fail(Errors.UNSUPPORTED_VERSION.exception());
            return;
        }
        ApiVersionsResponseData data = new ApiVersionsResponseData();
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey.minRequiredInterBrokerMagic <= RecordBatch.CURRENT_MAGIC_VALUE) {
                ApiVersionsResponseData.ApiVersion apiVersionData =
                        new ApiVersionsResponseData.ApiVersion()
                                .setApiKey(apiKey.id)
                                .setMinVersion(apiKey.oldestVersion())
                                .setMaxVersion(apiKey.latestVersion());
                if (apiKey.equals(ApiKeys.METADATA)) {
                    // Not support TopicId
                    short v = apiKey.latestVersion() > 11 ? 11 : apiKey.latestVersion();
                    apiVersionData.setMaxVersion(v);
                } else if (apiKey.equals(ApiKeys.FETCH)) {
                    // Not support TopicId
                    short v = apiKey.latestVersion() > 12 ? 12 : apiKey.latestVersion();
                    apiVersionData.setMaxVersion(v);
                }
                data.apiKeys().add(apiVersionData);
            }
        }
        request.complete(new ApiVersionsResponse(data));
    }

    void handleProducerRequest(KafkaRequest request) {
        ProduceRequest produceRequest = request.request();
        ProduceRequestData produceData = produceRequest.data();
        short acks = produceData.acks();
        int timeout = produceData.timeoutMs();

        LOG.debug("Handling Kafka produce request with acks={}, timeout={}", acks, timeout);

        // Track all topic-partition requests
        Map<TopicPartition, CompletableFuture<ProduceResponseData.PartitionProduceResponse>>
                pendingResponses = new HashMap<>();
        AtomicInteger pendingCount = new AtomicInteger();

        // Process each topic
        for (ProduceRequestData.TopicProduceData topicData : produceData.topicData()) {
            String topic = topicData.name();

            // Get table ID from metadata cache
            Long tableId = null;
            if (metadataCache != null) {
                try {
                    tableId = metadataCache.getOrFetchTableId(topic).get();
                } catch (Exception e) {
                    LOG.error("Error fetching table ID for topic: {}", topic, e);
                }
            }

            if (tableId == null) {
                LOG.warn("Table not found for topic: {}", topic);
                // Create error responses for all partitions
                for (ProduceRequestData.PartitionProduceData partitionData :
                        topicData.partitionData()) {
                    TopicPartition tp = new TopicPartition(topic, partitionData.index());
                    CompletableFuture<ProduceResponseData.PartitionProduceResponse> future =
                            new CompletableFuture<>();
                    future.complete(
                            new ProduceResponseData.PartitionProduceResponse()
                                    .setIndex(partitionData.index())
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                    pendingResponses.put(tp, future);
                }
                continue;
            }

            // Group partitions by their target Fluss bucket
            Map<TableBucket, BytesView> bucketRecords = new HashMap<>();
            Map<TableBucket, TopicPartition> bucketToTopicPartition = new HashMap<>();

            for (ProduceRequestData.PartitionProduceData partitionData :
                    topicData.partitionData()) {
                int partition = partitionData.index();
                TopicPartition tp = new TopicPartition(topic, partition);

                // Get records for this partition
                MemoryRecords kafkaRecords = (MemoryRecords) partitionData.records();
                if (kafkaRecords == null) {
                    CompletableFuture<ProduceResponseData.PartitionProduceResponse> future =
                            new CompletableFuture<>();
                    future.complete(
                            new ProduceResponseData.PartitionProduceResponse()
                                    .setIndex(partition)
                                    .setErrorCode(Errors.NONE.code())
                                    .setBaseOffset(0L));
                    pendingResponses.put(tp, future);
                    continue;
                }

                // Convert Kafka partition to Fluss bucket
                int bucketId = KafkaProtocolUtils.kafkaPartitionToBucket(partition);
                TableBucket tableBucket = new TableBucket(tableId, bucketId);

                // Convert Kafka records to Fluss BytesView
                BytesView flussRecords = KafkaProtocolUtils.kafkaRecordsToBytesView(kafkaRecords);
                bucketRecords.put(tableBucket, flussRecords);
                bucketToTopicPartition.put(tableBucket, tp);

                // Initialize pending future
                pendingResponses.put(tp, new CompletableFuture<>());
                pendingCount.incrementAndGet();
            }

            if (bucketRecords.isEmpty()) {
                continue;
            }

            // Create Fluss ProduceLogRequest
            ProduceLogRequest flussRequest =
                    new ProduceLogRequest()
                            .setTableId(tableId)
                            .setAcks((int) acks)
                            .setTimeoutMs(timeout);

            for (Map.Entry<TableBucket, BytesView> bucketEntry : bucketRecords.entrySet()) {
                TableBucket bucket = bucketEntry.getKey();
                BytesView records = bucketEntry.getValue();

                PbProduceLogReqForBucket bucketReq =
                        flussRequest
                                .addBucketsReq()
                                .setBucketId(bucket.getBucket())
                                .setRecordsBytesView(records);

                if (bucket.getPartitionId() != null) {
                    bucketReq.setPartitionId(bucket.getPartitionId());
                }
            }

            // Send to Fluss and handle response
            final long finalTableId = tableId;
            gateway.produceLog(flussRequest)
                    .whenComplete(
                            (flussResponse, throwable) -> {
                                if (throwable != null) {
                                    LOG.error("Error producing to Fluss", throwable);
                                    // Set error for all buckets
                                    for (Map.Entry<TableBucket, TopicPartition> entry :
                                            bucketToTopicPartition.entrySet()) {
                                        TopicPartition tp = entry.getValue();
                                        CompletableFuture<
                                                        ProduceResponseData
                                                                .PartitionProduceResponse>
                                                future = pendingResponses.get(tp);
                                        if (future != null) {
                                            future.complete(
                                                    new ProduceResponseData
                                                                    .PartitionProduceResponse()
                                                            .setIndex(tp.partition())
                                                            .setErrorCode(
                                                                    Errors.forException(throwable)
                                                                            .code()));
                                        }
                                    }
                                } else {
                                    // Convert Fluss response to Kafka response
                                    for (PbProduceLogRespForBucket bucketResp :
                                            flussResponse.getBucketsRespsList()) {
                                        int bucketId = bucketResp.getBucketId();
                                        TableBucket bucket =
                                                new TableBucket(finalTableId, bucketId);
                                        TopicPartition tp = bucketToTopicPartition.get(bucket);

                                        if (tp != null) {
                                            ProduceResponseData.PartitionProduceResponse
                                                    partitionResp =
                                                            new ProduceResponseData
                                                                            .PartitionProduceResponse()
                                                                    .setIndex(tp.partition());

                                            if (bucketResp.hasErrorCode()) {
                                                partitionResp.setErrorCode(
                                                        (short) bucketResp.getErrorCode());
                                                if (bucketResp.hasErrorMessage()) {
                                                    partitionResp.setErrorMessage(
                                                            bucketResp.getErrorMessage());
                                                }
                                            } else {
                                                partitionResp.setErrorCode(Errors.NONE.code());
                                                if (bucketResp.hasBaseOffset()) {
                                                    partitionResp.setBaseOffset(
                                                            KafkaProtocolUtils
                                                                    .flussOffsetToKafkaOffset(
                                                                            bucketResp
                                                                                    .getBaseOffset()));
                                                }
                                            }

                                            CompletableFuture<
                                                            ProduceResponseData
                                                                    .PartitionProduceResponse>
                                                    future = pendingResponses.get(tp);
                                            if (future != null) {
                                                future.complete(partitionResp);
                                            }
                                        }
                                    }
                                }

                                // Check if all responses are complete
                                checkAndCompleteProduceRequest(request, pendingResponses);
                            });
        }

        // If no pending requests, complete immediately
        if (pendingCount.get() == 0) {
            completeProduceRequest(request, pendingResponses);
        }
    }

    private void checkAndCompleteProduceRequest(
            KafkaRequest request,
            Map<TopicPartition, CompletableFuture<ProduceResponseData.PartitionProduceResponse>>
                    pendingResponses) {
        // Check if all futures are done
        boolean allDone = pendingResponses.values().stream().allMatch(CompletableFuture::isDone);
        if (allDone) {
            completeProduceRequest(request, pendingResponses);
        }
    }

    private void completeProduceRequest(
            KafkaRequest request,
            Map<TopicPartition, CompletableFuture<ProduceResponseData.PartitionProduceResponse>>
                    pendingResponses) {
        // Build final Kafka response
        ProduceResponseData responseData = new ProduceResponseData();
        Map<String, List<ProduceResponseData.PartitionProduceResponse>> topicResponses =
                new LinkedHashMap<>();

        for (Map.Entry<
                        TopicPartition,
                        CompletableFuture<ProduceResponseData.PartitionProduceResponse>>
                entry : pendingResponses.entrySet()) {
            try {
                ProduceResponseData.PartitionProduceResponse partitionResp = entry.getValue().get();
                topicResponses
                        .computeIfAbsent(entry.getKey().topic(), k -> new ArrayList<>())
                        .add(partitionResp);
            } catch (Exception e) {
                LOG.error("Error getting partition response", e);
                topicResponses
                        .computeIfAbsent(entry.getKey().topic(), k -> new ArrayList<>())
                        .add(
                                new ProduceResponseData.PartitionProduceResponse()
                                        .setIndex(entry.getKey().partition())
                                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()));
            }
        }

        for (Map.Entry<String, List<ProduceResponseData.PartitionProduceResponse>> entry :
                topicResponses.entrySet()) {
            responseData
                    .responses()
                    .add(
                            new ProduceResponseData.TopicProduceResponse()
                                    .setName(entry.getKey())
                                    .setPartitionResponses(entry.getValue()));
        }

        request.complete(new ProduceResponse(responseData));
    }

    void handleMetadataRequest(KafkaRequest request) {
        org.apache.kafka.common.requests.MetadataRequest metadataRequest = request.request();

        LOG.debug("Handling Kafka metadata request for topics: {}", metadataRequest.topics());

        // Convert Kafka topics to Fluss table paths
        MetadataRequest flussMetadataRequest = new MetadataRequest();
        if (metadataRequest.topics() != null) {
            for (String topic : metadataRequest.topics()) {
                TablePath tablePath = KafkaProtocolUtils.kafkaTopicToTablePath(topic);
                flussMetadataRequest
                        .addTablePath()
                        .setDatabaseName(tablePath.getDatabaseName())
                        .setTableName(tablePath.getTableName());
            }
        }

        // Send metadata request to Fluss
        if (adminGateway != null) {
            adminGateway
                    .metadata(flussMetadataRequest)
                    .whenComplete(
                            (flussResponse, throwable) -> {
                                if (throwable != null) {
                                    LOG.error("Error fetching metadata from Fluss", throwable);
                                    request.fail(throwable);
                                    return;
                                }

                                // Convert Fluss metadata response to Kafka metadata response
                                MetadataResponseData kafkaMetadataResponse =
                                        convertFlussMetadataToKafka(flussResponse);
                                request.complete(
                                        new org.apache.kafka.common.requests.MetadataResponse(
                                                kafkaMetadataResponse, request.apiVersion()));
                            });
        } else {
            LOG.error("AdminGateway not available for metadata request");
            request.fail(new UnsupportedOperationException("Metadata request not supported"));
        }
    }

    private MetadataResponseData convertFlussMetadataToKafka(MetadataResponse flussResponse) {
        MetadataResponseData kafkaResponse = new MetadataResponseData();

        // Convert broker/server information
        // IMPORTANT: We need to return the KAFKA listener port, not the default Fluss port
        for (PbServerNode serverNode : flussResponse.getTabletServersList()) {
            // Try to find KAFKA listener endpoint
            int kafkaPort = serverNode.getPort(); // Default to Fluss port
            String host = serverNode.getHost();

            // Parse listeners string to find KAFKA listener
            // Format: "FLUSS://host:port,CLIENT://host:port,KAFKA://host:port"
            if (serverNode.hasListeners()) {
                String listeners = serverNode.getListeners();
                String[] listenerArray = listeners.split(",");
                for (String listener : listenerArray) {
                    String trimmed = listener.trim();
                    if (trimmed.startsWith("KAFKA://")) {
                        // Extract host:port from "KAFKA://host:port"
                        String hostPort = trimmed.substring("KAFKA://".length());
                        int colonIndex = hostPort.lastIndexOf(":");
                        if (colonIndex > 0) {
                            host = hostPort.substring(0, colonIndex);
                            try {
                                kafkaPort = Integer.parseInt(hostPort.substring(colonIndex + 1));
                                LOG.debug(
                                        "Found KAFKA endpoint for node {}: {}:{}",
                                        serverNode.getNodeId(),
                                        host,
                                        kafkaPort);
                            } catch (NumberFormatException e) {
                                LOG.warn("Failed to parse KAFKA port from: {}", hostPort);
                            }
                        }
                        break;
                    }
                }
            }

            kafkaResponse
                    .brokers()
                    .add(
                            new MetadataResponseData.MetadataResponseBroker()
                                    .setNodeId(serverNode.getNodeId())
                                    .setHost(host)
                                    .setPort(kafkaPort)
                                    .setRack(serverNode.hasRack() ? serverNode.getRack() : null));
        }

        // Add coordinator as controller
        // IMPORTANT: AdminClient needs to find the controller in the broker list to send
        // CREATE_TOPICS
        if (flussResponse.hasCoordinatorServer()) {
            PbServerNode coordinator = flussResponse.getCoordinatorServer();
            kafkaResponse.setControllerId(coordinator.getNodeId());

            // Add coordinator to broker list so AdminClient can find it
            // Try to find KAFKA listener for coordinator, default to using any TabletServer's KAFKA
            // port
            int coordinatorKafkaPort = coordinator.getPort();
            String coordinatorHost = coordinator.getHost();

            // Parse coordinator listeners to find KAFKA port
            if (coordinator.hasListeners()) {
                String listeners = coordinator.getListeners();
                String[] listenerArray = listeners.split(",");
                for (String listener : listenerArray) {
                    String trimmed = listener.trim();
                    if (trimmed.startsWith("KAFKA://")) {
                        String hostPort = trimmed.substring("KAFKA://".length());
                        int colonIndex = hostPort.lastIndexOf(":");
                        if (colonIndex > 0) {
                            coordinatorHost = hostPort.substring(0, colonIndex);
                            try {
                                coordinatorKafkaPort =
                                        Integer.parseInt(hostPort.substring(colonIndex + 1));
                                LOG.debug(
                                        "Found KAFKA endpoint for coordinator {}: {}:{}",
                                        coordinator.getNodeId(),
                                        coordinatorHost,
                                        coordinatorKafkaPort);
                            } catch (NumberFormatException e) {
                                LOG.warn(
                                        "Failed to parse coordinator KAFKA port from: {}",
                                        hostPort);
                            }
                        }
                        break;
                    }
                }
            } else if (!kafkaResponse.brokers().isEmpty()) {
                // Fallback: if coordinator has no KAFKA listener, use first TabletServer's KAFKA
                // listener
                // This works because in Fluss, TabletServer can forward CREATE_TOPICS to
                // Coordinator
                for (MetadataResponseData.MetadataResponseBroker broker : kafkaResponse.brokers()) {
                    coordinatorHost = broker.host();
                    coordinatorKafkaPort = broker.port();
                    LOG.debug(
                            "Coordinator has no KAFKA listener, using first broker {}:{}",
                            coordinatorHost,
                            coordinatorKafkaPort);
                    break;
                }
            }

            // Add coordinator as a broker if not already present
            boolean coordinatorInBrokerList = false;
            for (MetadataResponseData.MetadataResponseBroker broker : kafkaResponse.brokers()) {
                if (broker.nodeId() == coordinator.getNodeId()) {
                    coordinatorInBrokerList = true;
                    // Update its host/port to KAFKA listener
                    broker.setHost(coordinatorHost);
                    broker.setPort(coordinatorKafkaPort);
                    break;
                }
            }

            if (!coordinatorInBrokerList && !kafkaResponse.brokers().isEmpty()) {
                // Use first TabletServer's KAFKA endpoint for CREATE_TOPICS
                // since TabletServer can delegate to Coordinator
                LOG.debug(
                        "Coordinator not in broker list, requests will go to TabletServer for delegation");
            }
        }

        // Convert table metadata to Kafka topic metadata
        for (PbTableMetadata tableMetadata : flussResponse.getTableMetadatasList()) {
            String topicName =
                    KafkaProtocolUtils.tablePathToKafkaTopic(
                            new TablePath(
                                    tableMetadata.getTablePath().getDatabaseName(),
                                    tableMetadata.getTablePath().getTableName()));

            MetadataResponseData.MetadataResponseTopic topic =
                    new MetadataResponseData.MetadataResponseTopic()
                            .setName(topicName)
                            .setErrorCode(Errors.NONE.code())
                            .setIsInternal(false);

            // Convert bucket metadata to partition metadata
            for (int i = 0; i < tableMetadata.getBucketMetadatasCount(); i++) {
                PbBucketMetadata bucketMetadata = tableMetadata.getBucketMetadataAt(i);
                MetadataResponseData.MetadataResponsePartition partition =
                        new MetadataResponseData.MetadataResponsePartition()
                                .setPartitionIndex(
                                        KafkaProtocolUtils.bucketToKafkaPartition(
                                                bucketMetadata.getBucketId()))
                                .setErrorCode(Errors.NONE.code());

                if (bucketMetadata.hasLeaderId()) {
                    partition.setLeaderId(bucketMetadata.getLeaderId());
                }

                // Set replica nodes
                int[] replicaIds = bucketMetadata.getReplicaIds();
                for (int replicaId : replicaIds) {
                    partition.replicaNodes().add(replicaId);
                    partition.isrNodes().add(replicaId); // Simplified: use all replicas as ISR
                }

                topic.partitions().add(partition);
            }
            kafkaResponse.topics().add(topic);
        }

        return kafkaResponse;
    }

    void handleFindCoordinatorRequest(KafkaRequest request) {
        org.apache.kafka.common.requests.FindCoordinatorRequest findCoordinatorRequest =
                request.request();

        LOG.debug(
                "Handling Kafka FIND_COORDINATOR request for key: {}, type: {}",
                findCoordinatorRequest.data().key(),
                findCoordinatorRequest.data().keyType());

        // Create response data
        org.apache.kafka.common.message.FindCoordinatorResponseData responseData =
                new org.apache.kafka.common.message.FindCoordinatorResponseData();

        if (adminGateway == null) {
            LOG.error("AdminGateway not available for FIND_COORDINATOR request");
            responseData.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());
            responseData.setErrorMessage("Coordinator not available");
            request.complete(
                    new org.apache.kafka.common.requests.FindCoordinatorResponse(responseData));
            return;
        }

        // Get coordinator information based on type
        byte coordinatorType = findCoordinatorRequest.data().keyType();

        if (coordinatorType == 0) { // GROUP
            // For consumer group, return this TabletServer as coordinator
            // Since we handle group coordination in KafkaRequestHandler
            adminGateway
                    .metadata(new MetadataRequest())
                    .whenComplete(
                            (metadataResponse, throwable) -> {
                                if (throwable != null) {
                                    LOG.error(
                                            "Error fetching metadata for FIND_COORDINATOR",
                                            throwable);
                                    responseData.setErrorCode(
                                            Errors.COORDINATOR_NOT_AVAILABLE.code());
                                    responseData.setErrorMessage(throwable.getMessage());
                                    request.complete(
                                            new org.apache.kafka.common.requests
                                                    .FindCoordinatorResponse(responseData));
                                    return;
                                }

                                // Return first TabletServer as coordinator
                                if (!metadataResponse.getTabletServersList().isEmpty()) {
                                    PbServerNode serverNode =
                                            metadataResponse.getTabletServersList().get(0);

                                    // Find KAFKA listener
                                    int kafkaPort = serverNode.getPort();
                                    String host = serverNode.getHost();

                                    if (serverNode.hasListeners()) {
                                        String listeners = serverNode.getListeners();
                                        String[] listenerArray = listeners.split(",");
                                        for (String listener : listenerArray) {
                                            String trimmed = listener.trim();
                                            if (trimmed.startsWith("KAFKA://")) {
                                                String hostPort =
                                                        trimmed.substring("KAFKA://".length());
                                                int colonIndex = hostPort.lastIndexOf(":");
                                                if (colonIndex > 0) {
                                                    host = hostPort.substring(0, colonIndex);
                                                    try {
                                                        kafkaPort =
                                                                Integer.parseInt(
                                                                        hostPort.substring(
                                                                                colonIndex + 1));
                                                    } catch (NumberFormatException e) {
                                                        LOG.warn(
                                                                "Failed to parse KAFKA port: {}",
                                                                hostPort);
                                                    }
                                                }
                                                break;
                                            }
                                        }
                                    }

                                    responseData.setNodeId(serverNode.getNodeId());
                                    responseData.setHost(host);
                                    responseData.setPort(kafkaPort);
                                    responseData.setErrorCode(Errors.NONE.code());

                                    LOG.debug(
                                            "Returning coordinator: node={}, host={}:{}",
                                            serverNode.getNodeId(),
                                            host,
                                            kafkaPort);
                                } else {
                                    responseData.setErrorCode(
                                            Errors.COORDINATOR_NOT_AVAILABLE.code());
                                    responseData.setErrorMessage("No coordinator available");
                                }

                                request.complete(
                                        new org.apache.kafka.common.requests
                                                .FindCoordinatorResponse(responseData));
                            });
        } else if (coordinatorType == 1) { // TRANSACTION
            // Transactions are not supported yet
            LOG.warn("Transaction coordinator requested, but transactions are not supported");
            responseData.setErrorCode(Errors.UNSUPPORTED_VERSION.code());
            responseData.setErrorMessage("Transactions are not supported");
            request.complete(
                    new org.apache.kafka.common.requests.FindCoordinatorResponse(responseData));
        } else {
            LOG.error("Unknown coordinator type: {}", coordinatorType);
            responseData.setErrorCode(Errors.INVALID_REQUEST.code());
            responseData.setErrorMessage("Unknown coordinator type: " + coordinatorType);
            request.complete(
                    new org.apache.kafka.common.requests.FindCoordinatorResponse(responseData));
        }
    }

    void handleListOffsetRequest(KafkaRequest request) {
        org.apache.kafka.common.requests.ListOffsetsRequest listOffsetsRequest = request.request();

        LOG.debug("Handling Kafka list offsets request");

        Map<TopicPartition, CompletableFuture<ListOffsetsResponseData.ListOffsetsPartitionResponse>>
                pendingResponses = new HashMap<>();

        // Process each topic
        for (org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic topic :
                listOffsetsRequest.topics()) {
            String topicName = topic.name();

            // Get table ID from metadata cache
            Long tableId = null;
            if (metadataCache != null) {
                try {
                    tableId = metadataCache.getOrFetchTableId(topicName).get();
                } catch (Exception e) {
                    LOG.error("Error fetching table ID for topic: {}", topicName, e);
                }
            }

            if (tableId == null) {
                LOG.warn("Table not found for topic: {}", topicName);
                continue;
            }

            for (org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
                    partition : topic.partitions()) {
                int partitionIndex = partition.partitionIndex();
                long timestamp = partition.timestamp();
                TopicPartition tp = new TopicPartition(topicName, partitionIndex);

                // Create Fluss ListOffsetsRequest
                ListOffsetsRequest flussRequest = new ListOffsetsRequest();
                flussRequest.setFollowerServerId(-1).setTableId(tableId);

                // Set bucket IDs
                int bucketId = KafkaProtocolUtils.kafkaPartitionToBucket(partitionIndex);
                flussRequest.setBucketIds(new int[] {bucketId});

                // Determine offset type based on timestamp
                // -2 = earliest, -1 = latest
                // OffsetSpec constants: LIST_EARLIEST_OFFSET = 0, LIST_LATEST_OFFSET = 1,
                // LIST_OFFSET_FROM_TIMESTAMP = 2
                if (timestamp
                        == org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_TIMESTAMP) {
                    flussRequest.setOffsetType(0); // LIST_EARLIEST_OFFSET
                } else if (timestamp
                        == org.apache.kafka.common.requests.ListOffsetsRequest.LATEST_TIMESTAMP) {
                    flussRequest.setOffsetType(1); // LIST_LATEST_OFFSET
                } else {
                    // Timestamp-based lookup
                    flussRequest.setOffsetType(2); // LIST_OFFSET_FROM_TIMESTAMP
                    flussRequest.setStartTimestamp(timestamp);
                }

                CompletableFuture<ListOffsetsResponseData.ListOffsetsPartitionResponse> future =
                        new CompletableFuture<>();
                pendingResponses.put(tp, future);

                // Send request to Fluss
                gateway.listOffsets(flussRequest)
                        .whenComplete(
                                (flussResponse, throwable) -> {
                                    ListOffsetsResponseData.ListOffsetsPartitionResponse
                                            partitionResponse =
                                                    new ListOffsetsResponseData
                                                                    .ListOffsetsPartitionResponse()
                                                            .setPartitionIndex(partitionIndex);

                                    if (throwable != null) {
                                        LOG.error("Error listing offsets from Fluss", throwable);
                                        partitionResponse.setErrorCode(
                                                Errors.forException(throwable).code());
                                    } else if (!flussResponse.getBucketsRespsList().isEmpty()) {
                                        PbListOffsetsRespForBucket bucketResp =
                                                flussResponse.getBucketsRespAt(0);
                                        if (bucketResp.hasErrorCode()) {
                                            partitionResponse.setErrorCode(
                                                    (short) bucketResp.getErrorCode());
                                        } else {
                                            partitionResponse.setErrorCode(Errors.NONE.code());
                                            if (bucketResp.hasOffset()) {
                                                partitionResponse.setOffset(
                                                        KafkaProtocolUtils.flussOffsetToKafkaOffset(
                                                                bucketResp.getOffset()));
                                            }
                                            // Set timestamp to -1 if not available
                                            partitionResponse.setTimestamp(-1L);
                                        }
                                    }

                                    future.complete(partitionResponse);
                                    checkAndCompleteListOffsetsRequest(request, pendingResponses);
                                });
            }
        }
    }

    private void checkAndCompleteListOffsetsRequest(
            KafkaRequest request,
            Map<
                            TopicPartition,
                            CompletableFuture<ListOffsetsResponseData.ListOffsetsPartitionResponse>>
                    pendingResponses) {
        boolean allDone = pendingResponses.values().stream().allMatch(CompletableFuture::isDone);
        if (allDone) {
            // Build response
            ListOffsetsResponseData responseData = new ListOffsetsResponseData();
            Map<String, List<ListOffsetsResponseData.ListOffsetsPartitionResponse>> topicResponses =
                    new LinkedHashMap<>();

            for (Map.Entry<
                            TopicPartition,
                            CompletableFuture<ListOffsetsResponseData.ListOffsetsPartitionResponse>>
                    entry : pendingResponses.entrySet()) {
                try {
                    ListOffsetsResponseData.ListOffsetsPartitionResponse partitionResp =
                            entry.getValue().get();
                    topicResponses
                            .computeIfAbsent(entry.getKey().topic(), k -> new ArrayList<>())
                            .add(partitionResp);
                } catch (Exception e) {
                    LOG.error("Error getting partition response", e);
                }
            }

            for (Map.Entry<String, List<ListOffsetsResponseData.ListOffsetsPartitionResponse>>
                    entry : topicResponses.entrySet()) {
                responseData
                        .topics()
                        .add(
                                new ListOffsetsResponseData.ListOffsetsTopicResponse()
                                        .setName(entry.getKey())
                                        .setPartitions(entry.getValue()));
            }

            request.complete(
                    new org.apache.kafka.common.requests.ListOffsetsResponse(responseData));
        }
    }

    void handleOffsetFetchRequest(KafkaRequest request) {
        org.apache.kafka.common.requests.OffsetFetchRequest offsetFetchRequest = request.request();

        LOG.debug(
                "Handling Kafka OFFSET_FETCH request for group: {}",
                offsetFetchRequest.data().groupId());

        String groupId = offsetFetchRequest.data().groupId();

        // Create response
        org.apache.kafka.common.message.OffsetFetchResponseData responseData =
                new org.apache.kafka.common.message.OffsetFetchResponseData();

        // Fetch offsets for all requested topics/partitions
        for (org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic topic :
                offsetFetchRequest.data().topics()) {
            String topicName = topic.name();

            org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic
                    responseTopic =
                            new org.apache.kafka.common.message.OffsetFetchResponseData
                                            .OffsetFetchResponseTopic()
                                    .setName(topicName);

            for (int partitionIndex : topic.partitionIndexes()) {
                OffsetManager.OffsetAndMetadata offsetAndMetadata =
                        offsetManager.fetchOffset(groupId, topicName, partitionIndex);

                org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition
                        partition =
                                new org.apache.kafka.common.message.OffsetFetchResponseData
                                                .OffsetFetchResponsePartition()
                                        .setPartitionIndex(partitionIndex);

                if (offsetAndMetadata != null) {
                    partition.setCommittedOffset(offsetAndMetadata.offset());
                    partition.setMetadata(
                            offsetAndMetadata.metadata() != null
                                    ? offsetAndMetadata.metadata()
                                    : "");
                    partition.setCommittedLeaderEpoch(-1); // Not supported yet
                    partition.setErrorCode(Errors.NONE.code());

                    LOG.debug(
                            "Fetched offset for group={}, topic={}, partition={}, offset={}",
                            groupId,
                            topicName,
                            partitionIndex,
                            offsetAndMetadata.offset());
                } else {
                    // No offset found, return -1
                    partition.setCommittedOffset(-1);
                    partition.setMetadata("");
                    partition.setCommittedLeaderEpoch(-1);
                    partition.setErrorCode(Errors.NONE.code());

                    LOG.debug(
                            "No offset found for group={}, topic={}, partition={}",
                            groupId,
                            topicName,
                            partitionIndex);
                }

                responseTopic.partitions().add(partition);
            }

            responseData.topics().add(responseTopic);
        }

        responseData.setErrorCode(Errors.NONE.code());
        request.complete(
                new org.apache.kafka.common.requests.OffsetFetchResponse(
                        responseData, request.apiVersion()));
    }

    void handleOffsetCommitRequest(KafkaRequest request) {
        org.apache.kafka.common.requests.OffsetCommitRequest offsetCommitRequest =
                request.request();

        LOG.debug(
                "Handling Kafka OFFSET_COMMIT request for group: {}",
                offsetCommitRequest.data().groupId());

        String groupId = offsetCommitRequest.data().groupId();

        // Create response
        org.apache.kafka.common.message.OffsetCommitResponseData responseData =
                new org.apache.kafka.common.message.OffsetCommitResponseData();

        // Commit offsets for all requested topics/partitions
        for (org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic
                topic : offsetCommitRequest.data().topics()) {
            String topicName = topic.name();

            org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic
                    responseTopic =
                            new org.apache.kafka.common.message.OffsetCommitResponseData
                                            .OffsetCommitResponseTopic()
                                    .setName(topicName);

            for (org.apache.kafka.common.message.OffsetCommitRequestData
                            .OffsetCommitRequestPartition
                    partition : topic.partitions()) {
                int partitionIndex = partition.partitionIndex();
                long offset = partition.committedOffset();
                String metadata = partition.committedMetadata();

                try {
                    // Commit offset
                    offsetManager.commitOffset(
                            groupId, topicName, partitionIndex, offset, metadata);

                    // Add success response
                    responseTopic
                            .partitions()
                            .add(
                                    new org.apache.kafka.common.message.OffsetCommitResponseData
                                                    .OffsetCommitResponsePartition()
                                            .setPartitionIndex(partitionIndex)
                                            .setErrorCode(Errors.NONE.code()));

                    LOG.debug(
                            "Committed offset for group={}, topic={}, partition={}, offset={}",
                            groupId,
                            topicName,
                            partitionIndex,
                            offset);
                } catch (Exception e) {
                    LOG.error(
                            "Error committing offset for group={}, topic={}, partition={}",
                            groupId,
                            topicName,
                            partitionIndex,
                            e);

                    // Add error response
                    responseTopic
                            .partitions()
                            .add(
                                    new org.apache.kafka.common.message.OffsetCommitResponseData
                                                    .OffsetCommitResponsePartition()
                                            .setPartitionIndex(partitionIndex)
                                            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()));
                }
            }

            responseData.topics().add(responseTopic);
        }

        request.complete(new org.apache.kafka.common.requests.OffsetCommitResponse(responseData));
    }

    void handleFetchRequest(KafkaRequest request) {
        org.apache.kafka.common.requests.FetchRequest fetchRequest = request.request();

        LOG.debug("Handling Kafka fetch request");

        // Create Fluss fetch request
        FetchLogRequest flussFetchRequest =
                new FetchLogRequest()
                        .setFollowerServerId(-1) // -1 indicates client request
                        .setMaxBytes(fetchRequest.maxBytes())
                        .setMaxWaitMs(fetchRequest.maxWait())
                        .setMinBytes(fetchRequest.minBytes());

        // Convert Kafka fetch requests to Fluss format
        Map<Long, PbFetchLogReqForTable> tableRequests = new HashMap<>();
        Map<TopicPartition, Long> topicPartitionToTableId = new HashMap<>();

        // Kafka 3.x uses data() to access the request data
        for (org.apache.kafka.common.message.FetchRequestData.FetchTopic topicData :
                fetchRequest.data().topics()) {
            String topic = topicData.topic();

            // Get table ID from metadata cache
            Long tableId = null;
            if (metadataCache != null) {
                try {
                    tableId = metadataCache.getOrFetchTableId(topic).get();
                } catch (Exception e) {
                    LOG.error("Error fetching table ID for topic: {}", topic, e);
                }
            }

            if (tableId == null) {
                LOG.warn("Table not found for topic: {}", topic);
                continue;
            }

            for (org.apache.kafka.common.message.FetchRequestData.FetchPartition partitionData :
                    topicData.partitions()) {
                int partition = partitionData.partition();
                TopicPartition tp = new TopicPartition(topic, partition);
                topicPartitionToTableId.put(tp, tableId);

                PbFetchLogReqForTable tableReq =
                        tableRequests.computeIfAbsent(
                                tableId,
                                id -> {
                                    return flussFetchRequest
                                            .addTablesReq()
                                            .setTableId(id)
                                            .setProjectionPushdownEnabled(false);
                                });

                tableReq.addBucketsReq()
                        .setBucketId(KafkaProtocolUtils.kafkaPartitionToBucket(partition))
                        .setFetchOffset(
                                KafkaProtocolUtils.kafkaOffsetToFlussOffset(
                                        partitionData.fetchOffset()))
                        .setMaxFetchBytes(partitionData.partitionMaxBytes());

                // Note: Leader epoch is not yet supported in Fluss FetchLogRequest (TODO in proto)
            }
        }

        // Send fetch request to Fluss
        gateway.fetchLog(flussFetchRequest)
                .whenComplete(
                        (flussResponse, throwable) -> {
                            if (throwable != null) {
                                LOG.error("Error fetching from Fluss", throwable);
                                request.fail(throwable);
                                return;
                            }

                            // Convert Fluss response to Kafka response
                            FetchResponseData kafkaFetchResponse =
                                    convertFlussFetchToKafka(
                                            flussResponse, topicPartitionToTableId);
                            request.complete(
                                    new org.apache.kafka.common.requests.FetchResponse(
                                            kafkaFetchResponse));
                        });
    }

    private FetchResponseData convertFlussFetchToKafka(
            FetchLogResponse flussResponse, Map<TopicPartition, Long> topicPartitionToTableId) {
        FetchResponseData kafkaResponse = new FetchResponseData();

        // Build reverse map: tableId -> topic
        Map<Long, String> tableIdToTopic = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : topicPartitionToTableId.entrySet()) {
            tableIdToTopic.putIfAbsent(entry.getValue(), entry.getKey().topic());
        }

        for (int i = 0; i < flussResponse.getTablesRespsCount(); i++) {
            PbFetchLogRespForTable tableResp = flussResponse.getTablesRespAt(i);
            long tableId = tableResp.getTableId();
            String topicName = tableIdToTopic.getOrDefault(tableId, "unknown_topic");

            FetchResponseData.FetchableTopicResponse topicResponse =
                    new FetchResponseData.FetchableTopicResponse().setTopic(topicName);

            for (int j = 0; j < tableResp.getBucketsRespsCount(); j++) {
                PbFetchLogRespForBucket bucketResp = tableResp.getBucketsRespAt(j);
                FetchResponseData.PartitionData partitionData =
                        new FetchResponseData.PartitionData()
                                .setPartitionIndex(
                                        KafkaProtocolUtils.bucketToKafkaPartition(
                                                bucketResp.getBucketId()));

                if (bucketResp.hasErrorCode()) {
                    partitionData.setErrorCode((short) bucketResp.getErrorCode());
                } else {
                    partitionData.setErrorCode(Errors.NONE.code());

                    if (bucketResp.hasHighWatermark()) {
                        partitionData.setHighWatermark(
                                KafkaProtocolUtils.flussOffsetToKafkaOffset(
                                        bucketResp.getHighWatermark()));
                    }

                    if (bucketResp.hasRecords()) {
                        // Convert Fluss records bytes to Kafka MemoryRecords
                        byte[] recordsBytes = bucketResp.getRecords();
                        MemoryRecords kafkaRecords =
                                MemoryRecords.readableRecords(ByteBuffer.wrap(recordsBytes));
                        partitionData.setRecords(kafkaRecords);
                    }
                }

                topicResponse.partitions().add(partitionData);
            }

            kafkaResponse.responses().add(topicResponse);
        }

        return kafkaResponse;
    }

    void handleJoinGroupRequest(KafkaRequest request) {}

    void handleSyncGroupRequest(KafkaRequest request) {}

    void handleHeartbeatRequest(KafkaRequest request) {}

    void handleLeaveGroupRequest(KafkaRequest request) {}

    void handleDescribeGroupsRequest(KafkaRequest request) {}

    void handleListGroupsRequest(KafkaRequest request) {}

    void handleDeleteGroupsRequest(KafkaRequest request) {}

    void handleSaslHandshakeRequest(KafkaRequest request) {}

    void handleSaslAuthenticateRequest(KafkaRequest request) {}

    void handleCreateTopicsRequest(KafkaRequest request) {
        org.apache.kafka.common.requests.CreateTopicsRequest createTopicsRequest =
                request.request();
        CreateTopicsRequestData requestData = createTopicsRequest.data();

        LOG.debug("Handling Kafka CREATE_TOPICS request");

        if (!supportsAdminWriteOps()) {
            LOG.error("Admin write operations not available for CREATE_TOPICS");
            request.fail(
                    new UnsupportedOperationException(
                            "CREATE_TOPICS not supported - please use Fluss Admin API"));
            return;
        }

        CreateTopicsResponseData responseData = new CreateTopicsResponseData();

        // Track all topic creation futures
        Map<String, CompletableFuture<CreateTopicsResponseData.CreatableTopicResult>>
                pendingTopics = new HashMap<>();

        for (CreateTopicsRequestData.CreatableTopic topicData : requestData.topics()) {
            String topicName = topicData.name();
            TablePath tablePath = KafkaProtocolUtils.kafkaTopicToTablePath(topicName);

            CompletableFuture<CreateTopicsResponseData.CreatableTopicResult> future =
                    new CompletableFuture<>();
            pendingTopics.put(topicName, future);

            // Build table descriptor
            int numBuckets =
                    topicData.numPartitions() > 0 ? topicData.numPartitions() : 3; // Default 3
            short replicationFactor =
                    topicData.replicationFactor() > 0
                            ? topicData.replicationFactor()
                            : (short) 1; // Default RF=1

            try {
                // Create simple key-value table for Kafka topics
                TableDescriptor descriptor =
                        createKafkaTopicTableDescriptor(numBuckets, replicationFactor);

                // Build Fluss CreateTableRequest
                CreateTableRequest flussRequest = new CreateTableRequest();
                flussRequest
                        .setTablePath()
                        .setDatabaseName(tablePath.getDatabaseName())
                        .setTableName(tablePath.getTableName());
                flussRequest.setTableJson(descriptor.toJsonBytes());
                flussRequest.setIgnoreIfExists(false);

                // Send request to Coordinator via TabletService delegation
                ((org.apache.fluss.server.tablet.TabletService) gateway)
                        .createTableViaCoordinator(flussRequest)
                        .whenComplete(
                                (flussResponse, throwable) -> {
                                    CreateTopicsResponseData.CreatableTopicResult topicResult =
                                            new CreateTopicsResponseData.CreatableTopicResult()
                                                    .setName(topicName);

                                    if (throwable != null) {
                                        LOG.error("Error creating topic: {}", topicName, throwable);
                                        topicResult.setErrorCode(
                                                Errors.forException(throwable).code());
                                        topicResult.setErrorMessage(throwable.getMessage());
                                    } else {
                                        topicResult.setErrorCode(Errors.NONE.code());
                                        // Invalidate metadata cache
                                        if (metadataCache != null) {
                                            metadataCache.invalidate(topicName);
                                        }
                                    }

                                    future.complete(topicResult);
                                    checkAndCompleteCreateTopicsRequest(request, pendingTopics);
                                });
            } catch (Exception e) {
                LOG.error("Error preparing create topic request for: {}", topicName, e);
                CreateTopicsResponseData.CreatableTopicResult topicResult =
                        new CreateTopicsResponseData.CreatableTopicResult()
                                .setName(topicName)
                                .setErrorCode(Errors.INVALID_REQUEST.code())
                                .setErrorMessage(e.getMessage());
                future.complete(topicResult);
            }
        }

        // If no topics, return immediately
        if (pendingTopics.isEmpty()) {
            request.complete(
                    new org.apache.kafka.common.requests.CreateTopicsResponse(responseData));
        }
    }

    private TableDescriptor createKafkaTopicTableDescriptor(
            int numBuckets, short replicationFactor) {
        // Kafka topics are schema-less, create a simple key-value table
        // with BYTES columns to store any data
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("key", org.apache.fluss.types.DataTypes.BYTES())
                                .column("value", org.apache.fluss.types.DataTypes.BYTES())
                                .build())
                .distributedBy(numBuckets)
                .property("replication.factor", String.valueOf(replicationFactor))
                .build();
    }

    private void checkAndCompleteCreateTopicsRequest(
            KafkaRequest request,
            Map<String, CompletableFuture<CreateTopicsResponseData.CreatableTopicResult>>
                    pendingTopics) {
        boolean allDone = pendingTopics.values().stream().allMatch(CompletableFuture::isDone);
        if (allDone) {
            CreateTopicsResponseData responseData = new CreateTopicsResponseData();
            for (Map.Entry<String, CompletableFuture<CreateTopicsResponseData.CreatableTopicResult>>
                    entry : pendingTopics.entrySet()) {
                try {
                    responseData.topics().add(entry.getValue().get());
                } catch (Exception e) {
                    LOG.error("Error getting topic creation result", e);
                }
            }
            request.complete(
                    new org.apache.kafka.common.requests.CreateTopicsResponse(responseData));
        }
    }

    void handleInitProducerIdRequest(KafkaRequest request) {}

    void handleAddPartitionsToTxnRequest(KafkaRequest request) {}

    void handleAddOffsetsToTxnRequest(KafkaRequest request) {}

    void handleTxnOffsetCommitRequest(KafkaRequest request) {}

    void handleEndTxnRequest(KafkaRequest request) {}

    void handleWriteTxnMarkersRequest(KafkaRequest request) {}

    void handleDescribeConfigsRequest(KafkaRequest request) {
        org.apache.kafka.common.requests.DescribeConfigsRequest describeConfigsRequest =
                request.request();

        LOG.debug("Handling Kafka DESCRIBE_CONFIGS request");

        org.apache.kafka.common.message.DescribeConfigsResponseData responseData =
                new org.apache.kafka.common.message.DescribeConfigsResponseData();

        // Process each requested resource
        for (org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource
                resource : describeConfigsRequest.data().resources()) {
            byte resourceType = resource.resourceType();
            String resourceName = resource.resourceName();

            org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsResult
                    result =
                            new org.apache.kafka.common.message.DescribeConfigsResponseData
                                            .DescribeConfigsResult()
                                    .setResourceType(resourceType)
                                    .setResourceName(resourceName)
                                    .setErrorCode(Errors.NONE.code());

            if (resourceType == 2) { // TOPIC
                // Return some basic topic configs
                result.configs()
                        .add(
                                createConfigEntry(
                                        "compression.type",
                                        "producer",
                                        false,
                                        false,
                                        false,
                                        "Compression type for topic"));
                result.configs()
                        .add(
                                createConfigEntry(
                                        "retention.ms",
                                        "604800000",
                                        false,
                                        false,
                                        false,
                                        "Retention time in milliseconds"));
                result.configs()
                        .add(
                                createConfigEntry(
                                        "segment.bytes",
                                        "1073741824",
                                        false,
                                        false,
                                        false,
                                        "Segment size in bytes"));
                result.configs()
                        .add(
                                createConfigEntry(
                                        "min.insync.replicas",
                                        "1",
                                        false,
                                        false,
                                        false,
                                        "Minimum number of replicas"));

                LOG.debug("Returned configs for topic: {}", resourceName);
            } else if (resourceType == 4) { // BROKER
                // Return some basic broker configs
                result.configs()
                        .add(
                                createConfigEntry(
                                        "num.network.threads",
                                        "3",
                                        true,
                                        false,
                                        false,
                                        "Number of network threads"));
                result.configs()
                        .add(
                                createConfigEntry(
                                        "num.io.threads",
                                        "8",
                                        true,
                                        false,
                                        false,
                                        "Number of I/O threads"));
                result.configs()
                        .add(
                                createConfigEntry(
                                        "socket.send.buffer.bytes",
                                        "102400",
                                        true,
                                        false,
                                        false,
                                        "Socket send buffer size"));
                result.configs()
                        .add(
                                createConfigEntry(
                                        "socket.receive.buffer.bytes",
                                        "102400",
                                        true,
                                        false,
                                        false,
                                        "Socket receive buffer size"));

                LOG.debug("Returned configs for broker: {}", resourceName);
            } else {
                result.setErrorCode(Errors.INVALID_REQUEST.code());
                result.setErrorMessage("Unsupported resource type: " + resourceType);
                LOG.warn("Unsupported resource type: {}", resourceType);
            }

            responseData.results().add(result);
        }

        request.complete(
                new org.apache.kafka.common.requests.DescribeConfigsResponse(responseData));
    }

    private org.apache.kafka.common.message.DescribeConfigsResponseData
                    .DescribeConfigsResourceResult
            createConfigEntry(
                    String name,
                    String value,
                    boolean readOnly,
                    boolean isDefault,
                    boolean isSensitive,
                    String documentation) {
        return new org.apache.kafka.common.message.DescribeConfigsResponseData
                        .DescribeConfigsResourceResult()
                .setName(name)
                .setValue(value)
                .setReadOnly(readOnly)
                .setIsDefault(isDefault)
                .setIsSensitive(isSensitive)
                .setDocumentation(documentation);
    }

    void handleAlterConfigsRequest(KafkaRequest request) {}

    void handleDeleteTopicsRequest(KafkaRequest request) {
        org.apache.kafka.common.requests.DeleteTopicsRequest deleteTopicsRequest =
                request.request();
        DeleteTopicsRequestData requestData = deleteTopicsRequest.data();

        LOG.debug("Handling Kafka DELETE_TOPICS request");

        if (!supportsAdminWriteOps()) {
            LOG.error("Admin write operations not available for DELETE_TOPICS");
            request.fail(
                    new UnsupportedOperationException(
                            "DELETE_TOPICS not supported - please use Fluss Admin API"));
            return;
        }

        DeleteTopicsResponseData responseData = new DeleteTopicsResponseData();

        // Track all topic deletion futures
        Map<String, CompletableFuture<DeleteTopicsResponseData.DeletableTopicResult>>
                pendingTopics = new HashMap<>();

        for (DeleteTopicsRequestData.DeleteTopicState topicData : requestData.topics()) {
            String topicName = topicData.name();
            TablePath tablePath = KafkaProtocolUtils.kafkaTopicToTablePath(topicName);

            CompletableFuture<DeleteTopicsResponseData.DeletableTopicResult> future =
                    new CompletableFuture<>();
            pendingTopics.put(topicName, future);

            // Build Fluss DropTableRequest
            DropTableRequest flussRequest = new DropTableRequest();
            flussRequest
                    .setTablePath()
                    .setDatabaseName(tablePath.getDatabaseName())
                    .setTableName(tablePath.getTableName());
            flussRequest.setIgnoreIfNotExists(false);

            // Send request to Coordinator via TabletService delegation
            ((org.apache.fluss.server.tablet.TabletService) gateway)
                    .dropTableViaCoordinator(flussRequest)
                    .whenComplete(
                            (flussResponse, throwable) -> {
                                DeleteTopicsResponseData.DeletableTopicResult topicResult =
                                        new DeleteTopicsResponseData.DeletableTopicResult()
                                                .setName(topicName);

                                if (throwable != null) {
                                    LOG.error("Error deleting topic: {}", topicName, throwable);
                                    topicResult.setErrorCode(Errors.forException(throwable).code());
                                    topicResult.setErrorMessage(throwable.getMessage());
                                } else {
                                    topicResult.setErrorCode(Errors.NONE.code());
                                    // Invalidate metadata cache
                                    if (metadataCache != null) {
                                        metadataCache.invalidate(topicName);
                                    }
                                }

                                future.complete(topicResult);
                                checkAndCompleteDeleteTopicsRequest(request, pendingTopics);
                            });
        }

        // If no topics, return immediately
        if (pendingTopics.isEmpty()) {
            request.complete(
                    new org.apache.kafka.common.requests.DeleteTopicsResponse(responseData));
        }
    }

    private void checkAndCompleteDeleteTopicsRequest(
            KafkaRequest request,
            Map<String, CompletableFuture<DeleteTopicsResponseData.DeletableTopicResult>>
                    pendingTopics) {
        boolean allDone = pendingTopics.values().stream().allMatch(CompletableFuture::isDone);
        if (allDone) {
            DeleteTopicsResponseData responseData = new DeleteTopicsResponseData();
            for (Map.Entry<String, CompletableFuture<DeleteTopicsResponseData.DeletableTopicResult>>
                    entry : pendingTopics.entrySet()) {
                try {
                    responseData.responses().add(entry.getValue().get());
                } catch (Exception e) {
                    LOG.error("Error getting topic deletion result", e);
                }
            }
            request.complete(
                    new org.apache.kafka.common.requests.DeleteTopicsResponse(responseData));
        }
    }

    void handleDeleteRecordsRequest(KafkaRequest request) {}

    void handleOffsetDeleteRequest(KafkaRequest request) {}

    void handleCreatePartitionsRequest(KafkaRequest request) {}

    void handleDescribeClusterRequest(KafkaRequest request) {
        LOG.debug("Handling Kafka DESCRIBE_CLUSTER request");

        if (adminGateway == null) {
            LOG.error("AdminGateway not available for DESCRIBE_CLUSTER request");
            request.fail(new UnsupportedOperationException("AdminGateway not available"));
            return;
        }

        // Fetch metadata to get cluster information
        adminGateway
                .metadata(new MetadataRequest())
                .whenComplete(
                        (metadataResponse, throwable) -> {
                            if (throwable != null) {
                                LOG.error(
                                        "Error fetching metadata for DESCRIBE_CLUSTER", throwable);
                                request.fail(throwable);
                                return;
                            }

                            org.apache.kafka.common.message.DescribeClusterResponseData
                                    responseData =
                                            new org.apache.kafka.common.message
                                                    .DescribeClusterResponseData();

                            // Set cluster ID (use a default one if not available)
                            responseData.setClusterId("fluss-cluster");

                            // Set controller ID (use coordinator server as controller)
                            if (metadataResponse.hasCoordinatorServer()) {
                                responseData.setControllerId(
                                        metadataResponse.getCoordinatorServer().getNodeId());
                            } else if (!metadataResponse.getTabletServersList().isEmpty()) {
                                // Fallback to first tablet server
                                responseData.setControllerId(
                                        metadataResponse.getTabletServersList().get(0).getNodeId());
                            }

                            // Add brokers (TabletServers with KAFKA listeners)
                            for (PbServerNode serverNode :
                                    metadataResponse.getTabletServersList()) {
                                // Find KAFKA listener
                                int kafkaPort = serverNode.getPort();
                                String host = serverNode.getHost();

                                if (serverNode.hasListeners()) {
                                    String listeners = serverNode.getListeners();
                                    String[] listenerArray = listeners.split(",");
                                    for (String listener : listenerArray) {
                                        String trimmed = listener.trim();
                                        if (trimmed.startsWith("KAFKA://")) {
                                            String hostPort =
                                                    trimmed.substring("KAFKA://".length());
                                            int colonIndex = hostPort.lastIndexOf(":");
                                            if (colonIndex > 0) {
                                                host = hostPort.substring(0, colonIndex);
                                                try {
                                                    kafkaPort =
                                                            Integer.parseInt(
                                                                    hostPort.substring(
                                                                            colonIndex + 1));
                                                } catch (NumberFormatException e) {
                                                    LOG.warn(
                                                            "Failed to parse KAFKA port: {}",
                                                            hostPort);
                                                }
                                            }
                                            break;
                                        }
                                    }
                                }

                                org.apache.kafka.common.message.DescribeClusterResponseData
                                                .DescribeClusterBroker
                                        broker =
                                                new org.apache.kafka.common.message
                                                                .DescribeClusterResponseData
                                                                .DescribeClusterBroker()
                                                        .setBrokerId(serverNode.getNodeId())
                                                        .setHost(host)
                                                        .setPort(kafkaPort);

                                if (serverNode.hasRack()) {
                                    broker.setRack(serverNode.getRack());
                                }

                                responseData.brokers().add(broker);
                            }

                            responseData.setErrorCode(Errors.NONE.code());

                            request.complete(
                                    new org.apache.kafka.common.requests.DescribeClusterResponse(
                                            responseData));

                            LOG.debug(
                                    "Returned cluster info: {} brokers",
                                    responseData.brokers().size());
                        });
    }
}
