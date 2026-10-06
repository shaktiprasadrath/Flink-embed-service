package com.shakti.flinkdemo.common;

import java.nio.file.Path;

/** Locates the sample input files. Override the folder with -Dflinkdemo.data.dir=... */
public final class DataFiles {

    public static final String ORDERS = "orders.csv";
    public static final String PAYMENTS = "payments.csv";
    public static final String RATES = "rates.csv";
    public static final String CUSTOMERS = "customers.csv";
    public static final String CLICKS = "clicks.jsonl";
    public static final String TXNS = "txns.csv";
    public static final String RULES = "rules.csv";
    public static final String AUTH_EVENTS = "auth_events.csv";
    public static final String SENSORS = "sensors.csv";
    public static final String USERS = "users.csv";

    private DataFiles() {}

    public static Path dir() {
        return Path.of(System.getProperty("flinkdemo.data.dir", "data")).toAbsolutePath();
    }

    public static Path path(String file) {
        return dir().resolve(file);
    }

    /** Path in the form Flink expects (file:///...), also safe on Windows. */
    public static String uri(String file) {
        return path(file).toUri().toString();
    }
}
