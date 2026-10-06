package com.shakti.flinkdemo.usecases.uc20_customio;

import com.shakti.flinkdemo.common.Db;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A custom sink written against Flink's Sink V2 API. Each parallel writer buffers rows and writes
 * them in one JDBC batch when Flink calls {@code flush}: on every checkpoint and at end of input.
 * Buffered rows are flushed before a checkpoint completes, which gives at-least-once delivery.
 */
public class BufferedH2Sink implements Sink<FileLine> {

    private final String table;
    private final int maxBuffered;

    public BufferedH2Sink(String table, int maxBuffered) {
        this.table = table;
        this.maxBuffered = maxBuffered;
    }

    @Override
    public SinkWriter<FileLine> createWriter(WriterInitContext context) throws IOException {
        try {
            return new Writer(context.getTaskInfo().getIndexOfThisSubtask());
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private class Writer implements SinkWriter<FileLine> {
        private final int subtask;
        private final Connection connection;
        private final PreparedStatement insert;
        private final List<FileLine> buffer = new ArrayList<>();

        Writer(int subtask) throws SQLException {
            this.subtask = subtask;
            this.connection = Db.connect();
            this.insert = connection.prepareStatement("INSERT INTO " + table + " VALUES (?, ?, ?, ?)");
        }

        @Override
        public void write(FileLine line, Context context) throws IOException {
            buffer.add(line);
            if (buffer.size() >= maxBuffered) {
                flush(false);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            if (buffer.isEmpty()) {
                return;
            }
            try {
                for (FileLine line : buffer) {
                    insert.setString(1, line.fileName());
                    insert.setLong(2, line.lineNo());
                    insert.setString(3, line.text());
                    insert.setInt(4, subtask);
                    insert.addBatch();
                }
                insert.executeBatch();
                buffer.clear();
            } catch (SQLException e) {
                throw new IOException("Writing to " + table + " failed", e);
            }
        }

        @Override
        public void close() throws Exception {
            insert.close();
            connection.close();
        }
    }
}
