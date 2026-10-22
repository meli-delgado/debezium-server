/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.libstreaming;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import io.debezium.DebeziumException;

/**
 * Fails startup when an ExtractNewRecordState transform would turn deletes into null-valued records: the
 * producer skips null values as tombstones, so those deletes would be silently lost downstream.
 */
final class DeleteHandlingGuard {

    private static final String EXTRACT_NEW_RECORD_STATE = "io.debezium.transforms.ExtractNewRecordState";
    private static final String DELETE_HANDLING_MODE = "delete.tombstone.handling.mode";
    private static final String DEFAULT_DELETE_HANDLING_MODE = "tombstone";
    private static final Set<String> NULL_VALUED_DELETE_MODES = Set.of("tombstone", "delete-to-tombstone");

    private DeleteHandlingGuard() {
    }

    static void rejectNullValuedDeletes(Function<String, Optional<String>> property) {
        for (String transform : transformNames(property)) {
            String prefix = "debezium.transforms." + transform + ".";
            if (!property.apply(prefix + "type").map(String::trim).filter(EXTRACT_NEW_RECORD_STATE::equals).isPresent()) {
                continue;
            }
            String mode = property.apply(prefix + DELETE_HANDLING_MODE).map(String::trim).orElse(DEFAULT_DELETE_HANDLING_MODE);
            if (NULL_VALUED_DELETE_MODES.contains(mode)) {
                throw new DebeziumException("Transform '" + transform + "' uses " + DELETE_HANDLING_MODE + "=" + mode
                        + ", which turns deletes into null-valued records the libstreaming producer cannot publish; "
                        + "set it to rewrite or rewrite-with-tombstone");
            }
        }
    }

    private static List<String> transformNames(Function<String, Optional<String>> property) {
        return property.apply("debezium.transforms")
                .map(names -> Arrays.stream(names.split(",")).map(String::trim).filter(name -> !name.isEmpty()).toList())
                .orElse(List.of());
    }
}
