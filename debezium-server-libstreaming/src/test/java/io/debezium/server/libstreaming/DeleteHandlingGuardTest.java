/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.libstreaming;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

public class DeleteHandlingGuardTest {

    private static void check(Map<String, String> properties) {
        DeleteHandlingGuard.rejectNullValuedDeletes(name -> Optional.ofNullable(properties.get(name)));
    }

    private static Map<String, String> unwrapWithMode(String mode) {
        Map<String, String> properties = new HashMap<>();
        properties.put("debezium.transforms", "route, unwrap");
        properties.put("debezium.transforms.route.type", "org.apache.kafka.connect.transforms.RegexRouter");
        properties.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        if (mode != null) {
            properties.put("debezium.transforms.unwrap.delete.tombstone.handling.mode", mode);
        }
        return properties;
    }

    @Test
    public void rejectsExtractNewRecordStateWithDefaultTombstoneMode() {
        assertThatThrownBy(() -> check(unwrapWithMode(null)))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("unwrap")
                .hasMessageContaining("tombstone");
    }

    @Test
    public void rejectsExtractNewRecordStateWithDeleteToTombstoneMode() {
        assertThatThrownBy(() -> check(unwrapWithMode("delete-to-tombstone")))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("delete-to-tombstone");
    }

    @Test
    public void acceptsExtractNewRecordStateThatKeepsDeletesAsRecords() {
        assertThatCode(() -> check(unwrapWithMode("rewrite"))).doesNotThrowAnyException();
        assertThatCode(() -> check(unwrapWithMode("rewrite-with-tombstone"))).doesNotThrowAnyException();
    }

    @Test
    public void acceptsConfigurationsWithoutExtractNewRecordState() {
        assertThatCode(() -> check(Map.of())).doesNotThrowAnyException();
        assertThatCode(() -> check(Map.of(
                "debezium.transforms", "route",
                "debezium.transforms.route.type", "org.apache.kafka.connect.transforms.RegexRouter"))).doesNotThrowAnyException();
    }
}
