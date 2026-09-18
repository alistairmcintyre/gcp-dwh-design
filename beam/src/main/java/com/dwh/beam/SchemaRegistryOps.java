package com.dwh.beam;

import org.apache.avro.Schema;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/** Register and read schema versions, the way a producer's CI pipeline would. */
public final class SchemaRegistryOps {

    /** v1: the shape before the change. */
    public static final String V1 = """
        {"type":"record","name":"Trade","namespace":"trading","fields":[
          {"name":"trade_id","type":"string"},
          {"name":"client_id","type":"string"},
          {"name":"instrument_id","type":"string"},
          {"name":"quantity","type":"long"},
          {"name":"executed_at","type":{"type":"long","logicalType":"timestamp-micros"}},
          {"name":"produced_at","type":{"type":"long","logicalType":"timestamp-micros"}}
        ]}""";

    /** v2: one added optional decimal. The only shape BACKWARD compatibility accepts. */
    public static final String V2 = """
        {"type":"record","name":"Trade","namespace":"trading","fields":[
          {"name":"trade_id","type":"string"},
          {"name":"client_id","type":"string"},
          {"name":"instrument_id","type":"string"},
          {"name":"quantity","type":"long"},
          {"name":"executed_at","type":{"type":"long","logicalType":"timestamp-micros"}},
          {"name":"produced_at","type":{"type":"long","logicalType":"timestamp-micros"}},
          {"name":"fill_price","default":null,
           "type":["null",{"type":"bytes","logicalType":"decimal","precision":20,"scale":6}]}
        ]}""";

    private SchemaRegistryOps() {}

    public static void register(String registryUrl, String subject, String version) throws Exception {
        String schema = version.equals("v1") ? V1 : V2;
        String body = "{\"schema\": " + jsonString(schema) + "}";
        HttpResponse<String> r = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder()
                        .uri(URI.create(registryUrl + "/subjects/" + subject + "/versions"))
                        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != 200) {
            throw new IllegalStateException("register " + version + " failed: " + r.body());
        }
    }

    /** Soft then hard delete, so a rerun starts from version 1 again. A missing subject is fine. */
    public static void deleteSubject(String registryUrl, String subject) throws Exception {
        for (String suffix : new String[] {"", "?permanent=true"}) {
            HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(registryUrl + "/subjects/" + subject + suffix))
                            .DELETE().build(),
                    HttpResponse.BodyHandlers.ofString());
        }
    }

    public static int latestId(String registryUrl, String topic) throws Exception {
        String body = get(registryUrl + "/subjects/" + topic + "-value/versions/latest");
        int i = body.indexOf("\"id\"");
        int start = body.indexOf(':', i) + 1;
        int end = start;
        while (end < body.length() && (Character.isDigit(body.charAt(end)) || body.charAt(end) == ' ')) {
            end++;
        }
        return Integer.parseInt(body.substring(start, end).trim());
    }

    public static Schema latestSchema(String registryUrl, String topic) throws Exception {
        String body = get(registryUrl + "/subjects/" + topic + "-value/versions/latest");
        return new Schema.Parser().parse(HttpSchemaRegistryClient.extractSchemaField(body));
    }

    private static String get(String url) throws Exception {
        HttpResponse<String> r = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder().uri(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != 200) {
            throw new IllegalStateException("GET " + url + " -> " + r.statusCode() + " " + r.body());
        }
        return r.body();
    }

    /** Minimal JSON string escaping -- the schema goes inside a JSON field. */
    static String jsonString(String s) {
        StringBuilder b = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"':  b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n");  break;
                case '\r': b.append("\\r");  break;
                case '\t': b.append("\\t");  break;
                default:   b.append(c);
            }
        }
        return b.append('"').toString();
    }
}
