package com.shakti.flinkdemo.common;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;

/** Factory for the official Flink JDBC connector sink, pointed at the in-memory H2 database. */
public final class JdbcSinks {

    private JdbcSinks() {}

    public static JdbcConnectionOptions h2() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(Db.URL)
                .withDriverName(Db.DRIVER)
                .withUsername(Db.USER)
                .withPassword(Db.PASSWORD)
                .build();
    }

    /**
     * At-least-once JDBC sink. Use an idempotent statement (H2 {@code MERGE INTO ... KEY(...)}) when
     * replays after a failure must not create duplicates.
     */
    public static <T> JdbcSink<T> sink(String sql, JdbcStatementBuilder<T> statementBuilder) {
        return sink(sql, statementBuilder, 100, 200);
    }

    public static <T> JdbcSink<T> sink(String sql, JdbcStatementBuilder<T> statementBuilder, int batchSize, long batchIntervalMs) {
        return JdbcSink.<T>builder()
                .withQueryStatement(sql, statementBuilder)
                .withExecutionOptions(JdbcExecutionOptions.builder()
                        .withBatchSize(batchSize)
                        .withBatchIntervalMs(batchIntervalMs)
                        .withMaxRetries(3)
                        .build())
                .buildAtLeastOnce(h2());
    }
}
