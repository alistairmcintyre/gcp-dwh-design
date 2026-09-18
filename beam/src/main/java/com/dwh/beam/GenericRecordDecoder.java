package com.dwh.beam;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;

/**
 * Decodes a Confluent-framed Kafka message into a {@link GenericRecord}.
 *
 * <p>THIS IS THE CLASS THAT DECIDES whether a new field survives. Decoding into a
 * {@code GenericRecord} keeps every field the writer sent, whatever the schema version. Decoding
 * into a fixed DTO with named getters -- which is the more natural thing to write -- silently drops
 * anything the DTO does not have a setter for, and no amount of DDL downstream brings it back:
 * the column gets added and stays null for ever.
 */
public class GenericRecordDecoder implements Serializable {

    private static final long serialVersionUID = 1L;

    private final AvroSchemaResolver resolver;
    private transient DecoderFactory decoderFactory;

    public GenericRecordDecoder(AvroSchemaResolver resolver) {
        this.resolver = resolver;
    }

    public GenericRecord decode(byte[] message) {
        int schemaId = ConfluentWireFormat.schemaId(message);
        Schema writer = resolver.byId(schemaId);

        if (decoderFactory == null) {
            decoderFactory = DecoderFactory.get();
        }
        // Reading with the writer's schema as both writer and reader keeps every field. Pinning a
        // reader schema here would reintroduce exactly the dropping this class exists to avoid.
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writer, writer);
        BinaryDecoder decoder =
                decoderFactory.binaryDecoder(ConfluentWireFormat.payload(message), null);
        try {
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not decode message with schema id " + schemaId, e);
        }
    }

    public int schemaId(byte[] message) {
        return ConfluentWireFormat.schemaId(message);
    }
}
