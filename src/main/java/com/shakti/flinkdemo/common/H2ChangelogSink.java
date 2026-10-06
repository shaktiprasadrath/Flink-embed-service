package com.shakti.flinkdemo.common;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Sink (Sink V2 API) that applies a Table API changelog stream to an H2 table as upserts:
 * INSERT / UPDATE_AFTER -> MERGE, DELETE -> DELETE, UPDATE_BEFORE -> ignored (the following
 * UPDATE_AFTER overwrites the row). Row fields must be in the same order as {@code columns}.
 * Run with parallelism 1 so changes for a key are applied in order.
 */
public class H2ChangelogSink implements Sink<Row> {

    private final String table;
    private final List<String> columns;
    private final List<String> keyColumns;

    public H2ChangelogSink(String table, List<String> columns, List<String> keyColumns) {
        this.table = table;
        this.columns = List.copyOf(columns);
        this.keyColumns = List.copyOf(keyColumns);
    }

    @Override
    public SinkWriter<Row> createWriter(WriterInitContext context) throws IOException {
        try {
            return new Writer();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private class Writer implements SinkWriter<Row> {
        private final Connection connection;
        private final PreparedStatement merge;
        private final PreparedStatement delete;
        private final int[] keyPositions;

        Writer() throws SQLException {
            connection = Db.connect();
            String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
            merge = connection.prepareStatement("MERGE INTO " + table + " (" + String.join(", ", columns)
                    + ") KEY (" + String.join(", ", keyColumns) + ") VALUES (" + placeholders + ")");
            delete = connection.prepareStatement("DELETE FROM " + table + " WHERE "
                    + keyColumns.stream().map(c -> c + " = ?").collect(Collectors.joining(" AND ")));
            keyPositions = keyColumns.stream().mapToInt(columns::indexOf).toArray();
            if (Arrays.stream(keyPositions).anyMatch(p -> p < 0)) {
                throw new IllegalArgumentException("Key columns " + keyColumns + " not in " + columns);
            }
        }

        @Override
        public void write(Row row, Context context) throws IOException {
            try {
                RowKind kind = row.getKind();
                if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
                    for (int i = 0; i < columns.size(); i++) {
                        merge.setObject(i + 1, row.getField(i));
                    }
                    merge.executeUpdate();
                } else if (kind == RowKind.DELETE) {
                    for (int i = 0; i < keyPositions.length; i++) {
                        delete.setObject(i + 1, row.getField(keyPositions[i]));
                    }
                    delete.executeUpdate();
                }
            } catch (SQLException e) {
                throw new IOException("Failed to apply " + row + " to " + table, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            // auto-commit: every statement is already durable in H2
        }

        @Override
        public void close() throws Exception {
            merge.close();
            delete.close();
            connection.close();
        }
    }
}
