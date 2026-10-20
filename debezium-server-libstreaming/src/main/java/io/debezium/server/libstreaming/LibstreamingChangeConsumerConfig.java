/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.libstreaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link LibstreamingChangeConsumer}.
 */
public class LibstreamingChangeConsumerConfig {

    public static final Field STREAM_ID = Field.create("stream.id")
            .withDisplayName("Default stream id")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The libstreaming stream id to publish change events to when a source table has no entry in stream.id.routes.");

    public static final Field STREAM_ID_ROUTES = Field.create("stream.id.routes")
            .withDisplayName("Per-table stream id routes")
            .withType(ConfigDef.Type.STRING)
            .withDefault("")
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Comma-separated list of destination=streamId pairs (e.g. myserver.public.orders=orders-stream) "
                    + "routing a source table's change events to a specific libstreaming stream. "
                    + "A destination without a match here falls back to stream.id.");

    public static final Field STREAMING_INSTANCE = Field.create("streaming.instance")
            .withDisplayName("Streaming instance")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The Streaming Platform instance used to resolve assignments for the stream (e.g. evp, shared, metrics).");

    public static final Field DATACENTER = Field.create("datacenter")
            .withDisplayName("Datacenter")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The current datacenter, passed to the libstreaming StreamProducerConfig.");

    public static final Field LANE_TYPE = Field.create("lane.type")
            .withDisplayName("Lane type")
            .withType(ConfigDef.Type.STRING)
            .withDefault("default")
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The traffic lane type to write change events to.");

    public static final Field SHARD_IDS = Field.create("shard.ids")
            .withDisplayName("Shard ids")
            .withType(ConfigDef.Type.STRING)
            .withDefault("")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Comma-separated shard ids shared by every target stream (e.g. 0,1,2). Each change event is written to the shard "
                    + "picked by hashing its key, so all changes to a row land on the same shard. When empty, or for events without a key, "
                    + "events are written unsharded.");

    public static final Field WAIT_MESSAGE_DELIVERY_TIMEOUT_MS = Field.create("wait.message.delivery.timeout.ms")
            .withDisplayName("Wait message delivery timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(30000)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Timeout in milliseconds to wait for message delivery confirmation.");

    public static final Field PRODUCER_READY_TIMEOUT_MS = Field.create("producer.ready.timeout.ms")
            .withDisplayName("Producer ready timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(30000)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Timeout in milliseconds to wait for the StreamProducer to pull in its initial assignments on startup.");

    private final String streamId;
    private final Map<String, String> streamIdRoutes;
    private final String streamingInstance;
    private final String datacenter;
    private final String laneType;
    private final List<String> shardIds;
    private final int waitMessageDeliveryTimeout;
    private final int producerReadyTimeout;

    public LibstreamingChangeConsumerConfig(Configuration config) {
        this.streamId = config.getString(STREAM_ID);
        this.streamIdRoutes = parseRoutes(config.getString(STREAM_ID_ROUTES));
        this.streamingInstance = config.getString(STREAMING_INSTANCE);
        this.datacenter = config.getString(DATACENTER);
        this.laneType = config.getString(LANE_TYPE);
        this.shardIds = parseShardIds(config.getString(SHARD_IDS));
        this.waitMessageDeliveryTimeout = config.getInteger(WAIT_MESSAGE_DELIVERY_TIMEOUT_MS);
        this.producerReadyTimeout = config.getInteger(PRODUCER_READY_TIMEOUT_MS);
    }

    private static Map<String, String> parseRoutes(String routes) {
        Map<String, String> parsed = new HashMap<>();
        if (routes == null || routes.isBlank()) {
            return parsed;
        }
        for (String entry : routes.split(",")) {
            String[] destinationAndStreamId = entry.trim().split("=", 2);
            if (destinationAndStreamId.length != 2 || destinationAndStreamId[0].isBlank() || destinationAndStreamId[1].isBlank()) {
                throw new IllegalArgumentException("Invalid stream.id.routes entry '" + entry + "', expected format <destination>=<streamId>");
            }
            parsed.put(destinationAndStreamId[0].trim(), destinationAndStreamId[1].trim());
        }
        return parsed;
    }

    private static List<String> parseShardIds(String shardIds) {
        if (shardIds == null || shardIds.isBlank()) {
            return List.of();
        }
        List<String> parsed = Arrays.stream(shardIds.split(",")).map(String::trim).toList();
        if (parsed.stream().anyMatch(String::isBlank)) {
            throw new IllegalArgumentException("Invalid shard.ids '" + shardIds + "', expected a comma-separated list of non-empty shard ids");
        }
        return parsed;
    }

    /**
     * Picks the shard for a change event key; the hash is deterministic across pods and restarts, so a row
     * always maps to the same shard. Empty when sharding is disabled or the event has no key.
     */
    public Optional<String> resolveShardId(byte[] key) {
        if (key == null || shardIds.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(shardIds.get(Math.floorMod(Arrays.hashCode(key), shardIds.size())));
    }

    /**
     * Resolves the libstreaming stream id a source table's change events should be published to,
     * falling back to {@link #getStreamId()} when {@code destination} has no entry in {@code stream.id.routes}.
     */
    public String resolveStreamId(String destination) {
        return streamIdRoutes.getOrDefault(destination, streamId);
    }

    public String getStreamId() {
        return streamId;
    }

    public Map<String, String> getStreamIdRoutes() {
        return streamIdRoutes;
    }

    public String getStreamingInstance() {
        return streamingInstance;
    }

    public String getDatacenter() {
        return datacenter;
    }

    public String getLaneType() {
        return laneType;
    }

    public List<String> getShardIds() {
        return shardIds;
    }

    public int getWaitMessageDeliveryTimeout() {
        return waitMessageDeliveryTimeout;
    }

    public int getProducerReadyTimeout() {
        return producerReadyTimeout;
    }
}
