package com.shakti.flinkdemo.common;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** All demo timestamps are "yyyy-MM-dd HH:mm:ss" in UTC, converted to epoch millis for Flink. */
public final class Times {

    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private Times() {}

    public static long parse(String text) {
        return LocalDateTime.parse(text.trim(), FORMAT).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    public static LocalDateTime toLocal(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }

    /** JDBC parameter for a TIMESTAMP column. */
    public static Timestamp sql(long epochMillis) {
        return Timestamp.valueOf(toLocal(epochMillis));
    }
}
