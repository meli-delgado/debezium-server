/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.libstreaming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;

public class LibstreamingChangeConsumerConfigTest {

    private LibstreamingChangeConsumerConfig configWith(String streamIdRoutes) {
        Map<String, String> props = new HashMap<>();
        props.put(LibstreamingChangeConsumerConfig.STREAMING_INSTANCE.name(), "evp");
        props.put(LibstreamingChangeConsumerConfig.DATACENTER.name(), "us1.staging.dog");
        if (streamIdRoutes != null) {
            props.put(LibstreamingChangeConsumerConfig.STREAM_ID_ROUTES.name(), streamIdRoutes);
        }
        return new LibstreamingChangeConsumerConfig(Configuration.from(props));
    }

    private LibstreamingChangeConsumerConfig configWithShardIds(String shardIds) {
        Map<String, String> props = new HashMap<>();
        props.put(LibstreamingChangeConsumerConfig.STREAM_ID_ROUTES.name(), "myserver.public.orders=orders-stream");
        props.put(LibstreamingChangeConsumerConfig.STREAMING_INSTANCE.name(), "evp");
        props.put(LibstreamingChangeConsumerConfig.DATACENTER.name(), "us1.staging.dog");
        props.put(LibstreamingChangeConsumerConfig.SHARD_IDS.name(), shardIds);
        return new LibstreamingChangeConsumerConfig(Configuration.from(props));
    }

    private static byte[] key(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void resolvesRoutedDestinationToItsMappedStreamId() {
        LibstreamingChangeConsumerConfig config = configWith(
                "myserver.public.orders=orders-stream,myserver.public.customers=customers-stream");

        assertThat(config.resolveStreamId("myserver.public.orders")).isEqualTo("orders-stream");
        assertThat(config.resolveStreamId("myserver.public.customers")).isEqualTo("customers-stream");
    }

    @Test
    public void rejectsMissingRoutes() {
        assertThatThrownBy(() -> configWith(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stream.id.routes must not be empty");
    }

    @Test
    public void rejectsBlankRoutes() {
        assertThatThrownBy(() -> configWith("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stream.id.routes must not be empty");
    }

    @Test
    public void rejectsUnroutedDestination() {
        LibstreamingChangeConsumerConfig config = configWith("myserver.public.orders=orders-stream");

        assertThatThrownBy(() -> config.resolveStreamId("myserver.public.line_items"))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("myserver.public.line_items");
    }

    @Test
    public void routesToTheSameStreamIdCanShareMultipleDestinations() {
        LibstreamingChangeConsumerConfig config = configWith(
                "myserver.public.orders=orders-stream,myserver.public.line_items=orders-stream");

        assertThat(config.resolveStreamId("myserver.public.orders")).isEqualTo("orders-stream");
        assertThat(config.resolveStreamId("myserver.public.line_items")).isEqualTo("orders-stream");
    }

    @Test
    public void rejectsRouteEntryMissingEqualsSign() {
        assertThatThrownBy(() -> configWith("myserver.public.orders"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("myserver.public.orders");
    }

    @Test
    public void rejectsRouteEntryWithBlankDestination() {
        assertThatThrownBy(() -> configWith("=orders-stream"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void rejectsRouteEntryWithBlankStreamId() {
        assertThatThrownBy(() -> configWith("myserver.public.orders="))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void rejectsDuplicateRouteDestinations() {
        assertThatThrownBy(() -> configWith("myserver.public.orders=orders-stream, myserver.public.orders=other-stream"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("myserver.public.orders");
    }

    @Test
    public void ignoresWhitespaceAroundRouteEntries() {
        LibstreamingChangeConsumerConfig config = configWith(" myserver.public.orders = orders-stream , myserver.public.customers=customers-stream ");

        assertThat(config.resolveStreamId("myserver.public.orders")).isEqualTo("orders-stream");
        assertThat(config.resolveStreamId("myserver.public.customers")).isEqualTo("customers-stream");
    }

    @Test
    public void resolvesTheSameKeyToTheSameShardAcrossConfigInstances() {
        Optional<String> first = configWithShardIds("0,1,2").resolveShardId(key("{\"id\":42}"));
        Optional<String> second = configWithShardIds("0,1,2").resolveShardId(key("{\"id\":42}"));

        assertThat(first).isPresent().isEqualTo(second);
    }

    @Test
    public void spreadsDistinctKeysAcrossEveryConfiguredShard() {
        LibstreamingChangeConsumerConfig config = configWithShardIds("0,1,2");
        Set<String> usedShards = new HashSet<>();

        for (int id = 0; id < 100; id++) {
            config.resolveShardId(key("{\"id\":" + id + "}")).ifPresent(usedShards::add);
        }

        assertThat(usedShards).containsExactlyInAnyOrder("0", "1", "2");
    }

    @Test
    public void resolvesNoShardForEventsWithoutKey() {
        assertThat(configWithShardIds("0,1,2").resolveShardId(null)).isEmpty();
    }

    @Test
    public void resolvesNoShardWhenShardIdsNotConfigured() {
        LibstreamingChangeConsumerConfig config = configWith("myserver.public.orders=orders-stream");

        assertThat(config.resolveShardId(key("{\"id\":42}"))).isEmpty();
        assertThat(config.getShardIds()).isEmpty();
    }

    @Test
    public void ignoresWhitespaceAroundShardIds() {
        assertThat(configWithShardIds(" shard-a , shard-b ").getShardIds()).containsExactly("shard-a", "shard-b");
    }

    @Test
    public void rejectsBlankShardIdEntry() {
        assertThatThrownBy(() -> configWithShardIds("0,,2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("0,,2");
    }
}
