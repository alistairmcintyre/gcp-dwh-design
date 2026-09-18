package com.dwh.beam;

import com.google.cloud.pubsub.v1.SchemaServiceClient;
import com.google.pubsub.v1.GetSchemaRequest;
import com.google.pubsub.v1.SchemaView;
import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pub/Sub's answer to a schema registry lookup. A message on a topic with a schema carries the
 * schema name and revision id as attributes instead of a 5-byte header, and a revision never
 * changes once committed, so each one is fetched once per worker and cached for good.
 */
public class PubsubSchemaResolver implements Serializable, AutoCloseable {

    private static final long serialVersionUID = 1L;

    public static final String SCHEMA_NAME = "googclient_schemaname";
    public static final String REVISION_ID = "googclient_schemarevisionid";

    private transient SchemaServiceClient client;
    private transient Map<String, Schema> cache;

    /** @param schemaName full name, projects/p/schemas/s */
    public Schema resolve(String schemaName, String revisionId) {
        if (cache == null) {
            cache = new ConcurrentHashMap<>();
        }
        return cache.computeIfAbsent(schemaName + "@" + revisionId, this::fetch);
    }

    private Schema fetch(String nameAtRevision) {
        try {
            if (client == null) {
                client = SchemaServiceClient.create();
            }
            String definition = client.getSchema(GetSchemaRequest.newBuilder()
                    .setName(nameAtRevision).setView(SchemaView.FULL).build()).getDefinition();
            return new Schema.Parser().parse(definition);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("cannot reach Pub/Sub schema service", e);
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
