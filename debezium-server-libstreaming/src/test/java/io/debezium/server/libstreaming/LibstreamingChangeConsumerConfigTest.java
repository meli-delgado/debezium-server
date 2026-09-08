/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.libstreaming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

public class LibstreamingChangeConsumerConfigTest {

    private LibstreamingChangeConsumerConfig configWith(String streamId, String streamIdRoutes) {
        Map<String, String> props = new HashMap<>();
        props.put(LibstreamingChangeConsumerConfig.STREAM_ID.name(), streamId);
        props.put(LibstreamingChangeConsumerConfig.STREAMING_INSTANCE.name(), "evp");
        props.put(LibstreamingChangeConsumerConfig.DATACENTER.name(), "us1.staging.dog");
        if (streamIdRoutes != null) {
            props.put(LibstreamingChangeConsumerConfig.STREAM_ID_ROUTES.name(), streamIdRoutes);
        }
        return new LibstreamingChangeConsumerConfig(Configuration.from(props));
    }

    @Test
    public void resolvesRoutedDestinationToItsMappedStreamId() {
        LibstreamingChangeConsumerConfig config = configWith("default-stream",
                "myserver.public.orders=orders-stream,myserver.public.customers=customers-stream");

        assertThat(config.resolveStreamId("myserver.public.orders")).isEqualTo("orders-stream");
        assertThat(config.resolveStreamId("myserver.public.customers")).isEqualTo("customers-stream");
    }

    @Test
    public void resolvesUnroutedDestinationToDefaultStreamId() {
        LibstreamingChangeConsumerConfig config = configWith("default-stream", "myserver.public.orders=orders-stream");

        assertThat(config.resolveStreamId("myserver.public.line_items")).isEqualTo("default-stream");
    }

    @Test
    public void resolvesEveryDestinationToDefaultStreamIdWhenNoRoutesConfigured() {
        LibstreamingChangeConsumerConfig config = configWith("default-stream", null);

        assertThat(config.resolveStreamId("myserver.public.orders")).isEqualTo("default-stream");
        assertThat(config.getStreamIdRoutes()).isEmpty();
    }

    @Test
    public void routesToTheSameStreamIdCanShareMultipleDestinations() {
        LibstreamingChangeConsumerConfig config = configWith("default-stream",
                "myserver.public.orders=orders-stream,myserver.public.line_items=orders-stream");

        assertThat(config.resolveStreamId("myserver.public.orders")).isEqualTo("orders-stream");
        assertThat(config.resolveStreamId("myserver.public.line_items")).isEqualTo("orders-stream");
    }

    @Test
    public void rejectsRouteEntryMissingEqualsSign() {
        assertThatThrownBy(() -> configWith("default-stream", "myserver.public.orders"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("myserver.public.orders");
    }

    @Test
    public void rejectsRouteEntryWithBlankDestination() {
        assertThatThrownBy(() -> configWith("default-stream", "=orders-stream"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void rejectsRouteEntryWithBlankStreamId() {
        assertThatThrownBy(() -> configWith("default-stream", "myserver.public.orders="))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void ignoresWhitespaceAroundRouteEntries() {
        LibstreamingChangeConsumerConfig config = configWith("default-stream", " myserver.public.orders = orders-stream , myserver.public.customers=customers-stream ");

        assertThat(config.resolveStreamId("myserver.public.orders")).isEqualTo("orders-stream");
        assertThat(config.resolveStreamId("myserver.public.customers")).isEqualTo("customers-stream");
    }
}
