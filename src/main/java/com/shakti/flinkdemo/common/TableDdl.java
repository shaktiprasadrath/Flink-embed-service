package com.shakti.flinkdemo.common;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** Flink SQL DDL for the sample files (filesystem connector + csv/json formats). */
public final class TableDdl {

    private TableDdl() {}

    /** Orders with event time + watermark, a processing-time column, and a view of the valid rows. */
    public static void orders(StreamTableEnvironment tEnv) {
        tEnv.executeSql("""
                CREATE TEMPORARY TABLE orders (
                  order_id    STRING,
                  customer_id STRING,
                  product     STRING,
                  quantity    INT,
                  price       DECIMAL(10, 2),
                  currency    STRING,
                  order_time_text STRING,
                  -- a rowtime must never be NULL, so malformed rows get a fallback time (and are filtered below)
                  order_time AS COALESCE(TO_TIMESTAMP(order_time_text), TIMESTAMP '1970-01-01 00:00:00'),
                  proc_time AS PROCTIME(),
                  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
                ) WITH (
                  'connector' = 'filesystem',
                  'path' = '%s',
                  'format' = 'csv',
                  'csv.allow-comments' = 'true',
                  'csv.ignore-parse-errors' = 'true'
                )""".formatted(DataFiles.uri(DataFiles.ORDERS)));
        // malformed lines become rows of NULLs (ignore-parse-errors); the view keeps only valid orders
        tEnv.executeSql("""
                CREATE TEMPORARY VIEW valid_orders AS
                SELECT * FROM orders
                WHERE order_id IS NOT NULL AND quantity > 0 AND TO_TIMESTAMP(order_time_text) IS NOT NULL""");
    }

    public static void clicks(StreamTableEnvironment tEnv) {
        tEnv.executeSql("""
                CREATE TEMPORARY TABLE clicks (
                  user_id STRING,
                  page    STRING,
                  ts      TIMESTAMP(3),
                  WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
                ) WITH (
                  'connector' = 'filesystem',
                  'path' = '%s',
                  'format' = 'json',
                  'json.timestamp-format.standard' = 'SQL'
                )""".formatted(DataFiles.uri(DataFiles.CLICKS)));
    }

    public static void rates(StreamTableEnvironment tEnv) {
        tEnv.executeSql("""
                CREATE TEMPORARY TABLE rates (
                  currency    STRING,
                  rate_to_usd DECIMAL(10, 4),
                  valid_from  TIMESTAMP(3),
                  WATERMARK FOR valid_from AS valid_from
                ) WITH (
                  'connector' = 'filesystem',
                  'path' = '%s',
                  'format' = 'csv',
                  'csv.allow-comments' = 'true'
                )""".formatted(DataFiles.uri(DataFiles.RATES)));
    }

    public static void users(StreamTableEnvironment tEnv) {
        tEnv.executeSql("""
                CREATE TEMPORARY TABLE users (
                  user_id   STRING,
                  full_name STRING,
                  email     STRING,
                  country   STRING,
                  interests STRING
                ) WITH (
                  'connector' = 'filesystem',
                  'path' = '%s',
                  'format' = 'csv',
                  'csv.allow-comments' = 'true'
                )""".formatted(DataFiles.uri(DataFiles.USERS)));
    }
}
