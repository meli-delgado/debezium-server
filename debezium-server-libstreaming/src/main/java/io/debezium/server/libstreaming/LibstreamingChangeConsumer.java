/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.libstreaming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dd.libstreaming.Libstreaming;
import com.dd.libstreaming.Observability;
import com.dd.libstreaming.Pod;
import com.dd.libstreaming.streamconsumer.Headers;
import com.dd.libstreaming.streamproducer.StreamProducer;
import com.dd.libstreaming.streamproducer.StreamProducerConfig;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.Header;
import io.debezium.server.BaseChangeConsumer;

/**
 * An implementation of the {@link DebeziumEngine.ChangeConsumer} interface that publishes change event messages
 * to a Streaming Platform stream via the libstreaming {@link StreamProducer}, instead of a Kafka topic.
 */
@Named("libstreaming")
@Dependent
public class LibstreamingChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LibstreamingChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.libstreaming.";

    private LibstreamingChangeConsumerConfig config;

    static final String TOMBSTONE_HEADER = "__debezium.tombstone";
    static final String TOMBSTONE_KEY_HEADER = "__debezium.key";

    private static final byte[] EMPTY_PAYLOAD = new byte[0];

    private final Map<String, StreamProducer> streamProducersByStreamId = new HashMap<>();

    @PostConstruct
    void start() {
        final Config mpConfig = ConfigProvider.getConfig();

        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new LibstreamingChangeConsumerConfig(configuration);

        Libstreaming.init(podFromEnv(), Observability.dogstatsdUnix(Observability.Level.INFO, "/var/run/datadog-agent/statsd.sock"));

        Set<String> streamIds = new HashSet<>(config.getStreamIdRoutes().values());
        streamIds.add(config.getStreamId());

        for (String streamId : streamIds) {
            streamProducersByStreamId.put(streamId, createReadyStreamProducer(streamId));
        }

        LOGGER.info("consumer started, publishing to streams {} via streaming instance '{}' across shards {}", streamProducersByStreamId.keySet(),
                config.getStreamingInstance(),
                config.getShardIds());
    }

    // Values are injected through the Kubernetes Downward API; unset vars fall back to "" as libstreaming expects.
    private static Pod podFromEnv() {
        return new Pod(
                envOrEmpty("POD_NAME"),
                envOrEmpty("K8S_CLUSTER_NAME"),
                envOrEmpty("AVAILABILITY_ZONE"),
                envOrEmpty("POD_IP"),
                envOrEmpty("NODE_NAME"),
                envOrEmpty("POD_NAMESPACE"),
                envOrEmpty("POD_SERVICE_ACCOUNT"));
    }

    private static String envOrEmpty(String name) {
        return Optional.ofNullable(System.getenv(name)).orElse("");
    }

    private StreamProducer createReadyStreamProducer(String streamId) {
        StreamProducerConfig.Builder streamProducerConfigBuilder = new StreamProducerConfig.Builder(config.getDatacenter());
        StreamProducer streamProducer = Libstreaming.newStreamProducer(streamId, config.getStreamingInstance(), streamProducerConfigBuilder.build());

        try {
            streamProducer.isReady().get(config.getProducerReadyTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DebeziumException(e);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new DebeziumException("StreamProducer did not become ready for stream '" + streamId + "'", e);
        }

        return streamProducer;
    }

    @PreDestroy
    void stop() {
        LOGGER.info("consumer destroyed...");
        for (StreamProducer streamProducer : streamProducersByStreamId.values()) {
            try {
                streamProducer.close();
            }
            catch (Throwable t) {
                LOGGER.warn("Could not close streamProducer", t);
            }
        }
    }

    @Override
    public void handleBatch(final List<ChangeEvent<Object, Object>> records,
                            final RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        final List<CompletableFuture<Void>> deliveryFutures = new ArrayList<>(records.size());
        final List<String> resolvedStreamIds = new ArrayList<>(records.size());

        for (ChangeEvent<Object, Object> record : records) {
            try {
                LOGGER.trace("Received event for destination '{}'", record.destination());
                Headers headers = convertToLibstreamingHeaders(record);

                String streamId = config.resolveStreamId(record.destination());
                resolvedStreamIds.add(streamId);
                StreamProducer streamProducer = streamProducersByStreamId.get(streamId);

                Optional<String> shardId = config.resolveShardId(record.key() == null ? null : getBytes(record.key()));

                deliveryFutures.add(streamProducer.write(payloadOf(record), config.getLaneType(), Optional.of(headers), Optional.empty(), shardId));
            }
            catch (Exception e) {
                throw new DebeziumException(e);
            }
        }

        try {
            for (int i = 0; i < records.size(); i++) {
                final var deliveryFuture = deliveryFutures.get(i);
                final var record = records.get(i);

                if (config.getWaitMessageDeliveryTimeout() == 0) {
                    deliveryFuture.get();
                }
                else {
                    try {
                        deliveryFuture.get(config.getWaitMessageDeliveryTimeout(), TimeUnit.MILLISECONDS);
                    }
                    catch (TimeoutException e) {
                        LOGGER.error("Timed out while waiting to publish a record for destination '{}' to stream '{}'", record.destination(), resolvedStreamIds.get(i));
                        throw new DebeziumException(e);
                    }
                }
                committer.markProcessed(record);
            }
        }
        catch (ExecutionException e) {
            throw new DebeziumException(e);
        }

        committer.markBatchFinished();
    }

    // libstreaming's Java API cannot publish a null payload, so tombstones go out empty, flagged by TOMBSTONE_HEADER
    // and carrying their key in TOMBSTONE_KEY_HEADER since the stream has no record key.
    private byte[] payloadOf(ChangeEvent<Object, Object> record) {
        return record.value() == null ? EMPTY_PAYLOAD : getBytes(record.value());
    }

    private Headers convertToLibstreamingHeaders(ChangeEvent<Object, Object> record) {
        Headers headers = Headers.empty();
        for (Header<Object> header : record.headers()) {
            headers.set(header.getKey(), new Headers.Value.StringValue(asString(header.getValue())));
        }
        if (record.value() == null) {
            headers.set(TOMBSTONE_HEADER, new Headers.Value.StringValue("true"));
            if (record.key() != null) {
                headers.set(TOMBSTONE_KEY_HEADER, new Headers.Value.StringValue(asString(record.key())));
            }
        }
        return headers;
    }
}
