package com.shakti.flinkdemo.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shakti.flinkdemo.common.Times;

/** One line of clicks.jsonl. */
public record Click(String userId, String page, long ts) {

    private static final ObjectMapper JSON = new ObjectMapper();

    public static Click parse(String jsonLine) {
        try {
            JsonNode n = JSON.readTree(jsonLine);
            return new Click(n.get("user_id").asText(), n.get("page").asText(), Times.parse(n.get("ts").asText()));
        } catch (Exception e) {
            throw new IllegalArgumentException("bad click json: " + jsonLine, e);
        }
    }
}
