/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.libstreaming;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link LibstreamingChangeConsumer}.
 */
public class LibstreamingChangeConsumerConfig {

    public static final Field STREAM_ID = Field.create("stream.id")
            .withDisplayName("Stream id")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The libstreaming stream id to publish change events to.");

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
    private final String streamingInstance;
    private final String datacenter;
    private final String laneType;
    private final int waitMessageDeliveryTimeout;
    private final int producerReadyTimeout;

    public LibstreamingChangeConsumerConfig(Configuration config) {
        this.streamId = config.getString(STREAM_ID);
        this.streamingInstance = config.getString(STREAMING_INSTANCE);
        this.datacenter = config.getString(DATACENTER);
        this.laneType = config.getString(LANE_TYPE);
        this.waitMessageDeliveryTimeout = config.getInteger(WAIT_MESSAGE_DELIVERY_TIMEOUT_MS);
        this.producerReadyTimeout = config.getInteger(PRODUCER_READY_TIMEOUT_MS);
    }

    public String getStreamId() {
        return streamId;
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

    public int getWaitMessageDeliveryTimeout() {
        return waitMessageDeliveryTimeout;
    }

    public int getProducerReadyTimeout() {
        return producerReadyTimeout;
    }
}
