/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.libstreaming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.dd.libstreaming.streamconsumer.Headers;
import com.dd.libstreaming.streamproducer.StreamProducer;

import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.Offsets;
import io.debezium.engine.DebeziumEngine.RecordCommitter;

public class LibstreamingChangeConsumerTest {

    private static final String KEY = "{\"id\":1}";

    private record TestChangeEvent(Object key, Object value, String destination) implements ChangeEvent<Object, Object> {
        @Override
        public Integer partition() {
            return null;
        }
    }

    private static class RecordingCommitter implements RecordCommitter<ChangeEvent<Object, Object>> {
        private final List<ChangeEvent<Object, Object>> processed = new ArrayList<>();
        private boolean batchFinished;

        @Override
        public void markProcessed(ChangeEvent<Object, Object> record) {
            processed.add(record);
        }

        @Override
        public void markBatchFinished() {
            batchFinished = true;
        }

        @Override
        public void markProcessed(ChangeEvent<Object, Object> record, Offsets offsets) {
            processed.add(record);
        }

        @Override
        public Offsets buildOffsets() {
            return null;
        }
    }

    private StreamProducer streamProducer;
    private LibstreamingChangeConsumer consumer;
    private RecordingCommitter committer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        streamProducer = mock(StreamProducer.class);
        when(streamProducer.write(any(byte[].class), anyString(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        // start() would initialize libstreaming, so inject the parsed config and a mocked producer instead.
        consumer = new LibstreamingChangeConsumer();
        setField("config", new LibstreamingChangeConsumerConfig(Configuration.from(Map.of(
                LibstreamingChangeConsumerConfig.STREAM_ID.name(), "default-stream",
                LibstreamingChangeConsumerConfig.STREAMING_INSTANCE.name(), "evp",
                LibstreamingChangeConsumerConfig.DATACENTER.name(), "us1.staging.dog",
                LibstreamingChangeConsumerConfig.SHARD_IDS.name(), "0,1,2"))));
        ((Map<String, StreamProducer>) getField("streamProducersByStreamId")).put("default-stream", streamProducer);
        committer = new RecordingCommitter();
    }

    private void setField(String name, Object value) throws Exception {
        Field field = LibstreamingChangeConsumer.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(consumer, value);
    }

    private Object getField(String name) throws Exception {
        Field field = LibstreamingChangeConsumer.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(consumer);
    }

    @SuppressWarnings("unchecked")
    private List<Write> captureWrites(int count) {
        ArgumentCaptor<byte[]> payloads = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<Optional<Headers>> headers = ArgumentCaptor.forClass(Optional.class);
        ArgumentCaptor<Optional<String>> shardIds = ArgumentCaptor.forClass(Optional.class);
        verify(streamProducer, times(count)).write(payloads.capture(), anyString(), headers.capture(), any(), shardIds.capture());

        List<Write> writes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            writes.add(new Write(payloads.getAllValues().get(i), headers.getAllValues().get(i).orElseThrow(), shardIds.getAllValues().get(i)));
        }
        return writes;
    }

    private record Write(byte[] payload, Headers headers, Optional<String> shardId) {
    }

    @Test
    public void keepsTombstoneEventsEnabledInTheEngine() {
        assertThat(consumer.supportsTombstoneEvents()).isTrue();
    }

    @Test
    public void publishesTombstoneAsEmptyPayloadFlaggedByHeader() throws Exception {
        ChangeEvent<Object, Object> tombstone = new TestChangeEvent(KEY, null, "myserver.public.orders");

        consumer.handleBatch(List.of(tombstone), committer);

        Write write = captureWrites(1).get(0);
        assertThat(write.payload()).isEmpty();
        assertThat(((Headers.Value.StringValue) write.headers().get(LibstreamingChangeConsumer.TOMBSTONE_HEADER)).get()).isEqualTo("true");
        assertThat(committer.processed).containsExactly(tombstone);
        assertThat(committer.batchFinished).isTrue();
    }

    @Test
    public void publishesRegularEventsWithoutTheTombstoneHeader() throws Exception {
        String value = "{\"id\":1,\"op\":\"u\"}";

        consumer.handleBatch(List.of(new TestChangeEvent(KEY, value, "myserver.public.orders")), committer);

        Write write = captureWrites(1).get(0);
        assertThat(new String(write.payload(), StandardCharsets.UTF_8)).isEqualTo(value);
        assertThat(write.headers().get(LibstreamingChangeConsumer.TOMBSTONE_HEADER)).isNull();
    }

    @Test
    public void routesATombstoneToTheSameShardAsItsDeleteEvent() throws Exception {
        consumer.handleBatch(List.of(
                new TestChangeEvent(KEY, "{\"id\":1,\"op\":\"d\"}", "myserver.public.orders"),
                new TestChangeEvent(KEY, null, "myserver.public.orders")), committer);

        List<Write> writes = captureWrites(2);
        assertThat(writes.get(0).shardId()).isPresent().isEqualTo(writes.get(1).shardId());
    }
}
