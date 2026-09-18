package com.dwh.beam;

import org.apache.avro.Schema;

import java.io.Serializable;

/**
 * Resolves a Confluent schema id to the schema that wrote the message.
 *
 * <p>In production this wraps {@code CachedSchemaRegistryClient}, which caches by id -- so an
 * unchanged stream costs one lookup ever, and a new version costs exactly one more. An interface so
 * the decoder is testable without a registry running.
 */
public interface AvroSchemaResolver extends Serializable {
    Schema byId(int schemaId);
}
