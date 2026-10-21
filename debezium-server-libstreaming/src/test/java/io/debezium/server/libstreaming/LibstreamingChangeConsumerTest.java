/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.libstreaming;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.Offsets;
import io.debezium.engine.DebeziumEngine.RecordCommitter;

public class LibstreamingChangeConsumerTest {

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

    // start() would initialize libstreaming, so inject only the parsed config.
    private static LibstreamingChangeConsumer consumerWithConfig() throws Exception {
        LibstreamingChangeConsumer consumer = new LibstreamingChangeConsumer();
        LibstreamingChangeConsumerConfig config = new LibstreamingChangeConsumerConfig(Configuration.from(Map.of(
                LibstreamingChangeConsumerConfig.STREAM_ID.name(), "default-stream",
                LibstreamingChangeConsumerConfig.STREAMING_INSTANCE.name(), "evp",
                LibstreamingChangeConsumerConfig.DATACENTER.name(), "us1.staging.dog")));
        Field configField = LibstreamingChangeConsumer.class.getDeclaredField("config");
        configField.setAccessible(true);
        configField.set(consumer, config);
        return consumer;
    }

    @Test
    public void declaresNoTombstoneSupportSoTheEngineDisablesThem() {
        assertThat(new LibstreamingChangeConsumer().supportsTombstoneEvents()).isFalse();
    }

    @Test
    public void skipsTombstonesAndStillMarksThemProcessed() throws Exception {
        LibstreamingChangeConsumer consumer = consumerWithConfig();
        RecordingCommitter committer = new RecordingCommitter();
        List<ChangeEvent<Object, Object>> tombstones = List.of(
                new TestChangeEvent("{\"id\":1}", null, "myserver.public.orders"),
                new TestChangeEvent(null, null, "myserver.public.orders"));

        consumer.handleBatch(tombstones, committer);

        assertThat(committer.processed).containsExactlyElementsOf(tombstones);
        assertThat(committer.batchFinished).isTrue();
    }
}
