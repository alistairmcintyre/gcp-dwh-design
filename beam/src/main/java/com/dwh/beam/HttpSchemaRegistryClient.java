package com.dwh.beam;

import org.apache.avro.Schema;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves schema ids against a Confluent Schema Registry over plain HTTP.
 *
 * <p>Deliberately not the Confluent client. That artifact lives on Confluent's own Maven repository,
 * which means adding a repository entry and a credential story before anything compiles. Resolving
 * an id is one GET returning one JSON field, so the dependency is not worth it here.
 *
 * <p>Caches by id, permanently. A schema id is immutable by definition -- registering a different
 * schema produces a different id -- so a cached entry can never go stale. That is what makes this
 * cheap in a streaming pipeline: a steady stream costs one lookup ever, and a new version costs
 * exactly one more.
 */
public class HttpSchemaRegistryClient implements AvroSchemaResolver, Serializable {

    private static final long serialVersionUID = 1L;

    private final String baseUrl;
    private transient Map<Integer, Schema> cache;
    private transient HttpClient http;

    public HttpSchemaRegistryClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    @Override
    public Schema byId(int schemaId) {
        if (cache == null) {
            cache = new ConcurrentHashMap<>();
        }
        return cache.computeIfAbsent(schemaId, this::fetch);
    }

    private Schema fetch(int schemaId) {
        if (http == null) {
            http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/schemas/ids/" + schemaId))
                .header("Accept", "application/vnd.schemaregistry.v1+json")
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "Schema registry returned " + response.statusCode() + " for id " + schemaId
                                + ": " + response.body());
            }
            return new Schema.Parser().parse(extractSchemaField(response.body()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted resolving schema id " + schemaId, e);
        } catch (Exception e) {
            throw new IllegalStateException("Could not resolve schema id " + schemaId, e);
        }
    }

    /**
     * Pulls the {@code schema} field out of the registry's response.
     *
     * <p>The body is {@code {"schema": "<the avro schema, JSON-escaped>"}}. Unescaping it by hand
     * rather than adding a JSON library: the field is the only thing in the response and the
     * escaping is the standard JSON set.
     */
    static String extractSchemaField(String body) {
        int key = body.indexOf("\"schema\"");
        if (key < 0) {
            throw new IllegalStateException("No schema field in registry response: " + body);
        }
        int firstQuote = body.indexOf('"', body.indexOf(':', key) + 1);
        StringBuilder out = new StringBuilder();
        for (int i = firstQuote + 1; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '\\') {
                char next = body.charAt(++i);
                switch (next) {
                    case 'n': out.append('\n'); break;
                    case 't': out.append('\t'); break;
                    case 'r': out.append('\r'); break;
                    case 'b': out.append('\b'); break;
                    case 'f': out.append('\f'); break;
                    case 'u':
                        out.append((char) Integer.parseInt(body.substring(i + 1, i + 5), 16));
                        i += 4;
                        break;
                    default: out.append(next);
                }
            } else if (c == '"') {
                return out.toString();
            } else {
                out.append(c);
            }
        }
        throw new IllegalStateException("Unterminated schema field in registry response");
    }
}
