package com.shakti.flinkdemo.common;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared H2 in-memory database. Flink runs as an embedded MiniCluster inside the same JVM,
 * so the Flink sinks and this helper see the same in-memory database.
 */
public final class Db {

    /** DB_CLOSE_DELAY=-1 keeps the in-memory DB alive for the whole JVM, not per connection. */
    public static final String URL = "jdbc:h2:mem:flinkdemo;DB_CLOSE_DELAY=-1";
    public static final String USER = "sa";
    public static final String PASSWORD = "";
    public static final String DRIVER = "org.h2.Driver";

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TS_MILLIS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private Db() {}

    public static Connection connect() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }

    public static void execute(String... statements) {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            for (String sql : statements) {
                s.execute(sql);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("SQL failed: " + String.join("; ", statements), e);
        }
    }

    /** Drops the table if it exists and creates it again, so every run starts clean. */
    public static void recreate(String table, String columnsDdl) {
        execute("DROP TABLE IF EXISTS " + table, "CREATE TABLE " + table + " (" + columnsDdl + ")");
    }

    /** Runs a query and returns each row as "v1|v2|..." for easy printing and assertions. */
    public static List<String> rows(String sql) {
        try (Connection c = connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                List<String> vals = new ArrayList<>(cols);
                for (int i = 1; i <= cols; i++) {
                    vals.add(format(rs.getObject(i)));
                }
                out.add(String.join("|", vals));
            }
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException("Query failed: " + sql, e);
        }
    }

    public static long count(String table) {
        return Long.parseLong(rows("SELECT COUNT(*) FROM " + table).get(0));
    }

    public static boolean tableExists(String table) {
        return !rows("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME) = '"
                + table.toUpperCase() + "'").isEmpty();
    }

    /** Prints a table as an aligned text grid. */
    public static void print(String table) {
        String sql = "SELECT * FROM " + table + " ORDER BY 1";
        try (Connection c = connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<String[]> grid = new ArrayList<>();
            String[] header = new String[cols];
            for (int i = 0; i < cols; i++) {
                header[i] = md.getColumnLabel(i + 1).toLowerCase();
            }
            grid.add(header);
            while (rs.next()) {
                String[] row = new String[cols];
                for (int i = 0; i < cols; i++) {
                    row[i] = format(rs.getObject(i + 1));
                }
                grid.add(row);
            }
            int[] width = new int[cols];
            for (String[] row : grid) {
                for (int i = 0; i < cols; i++) {
                    width[i] = Math.max(width[i], row[i].length());
                }
            }
            System.out.println("  table " + table + " (" + (grid.size() - 1) + " rows)");
            for (int r = 0; r < grid.size(); r++) {
                StringBuilder line = new StringBuilder("  ");
                for (int i = 0; i < cols; i++) {
                    line.append(String.format("%-" + width[i] + "s", grid.get(r)[i]));
                    if (i < cols - 1) {
                        line.append(" | ");
                    }
                }
                System.out.println(line);
                if (r == 0) {
                    System.out.println("  " + "-".repeat(line.length() - 2));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Query failed: " + sql, e);
        }
    }

    static String format(Object v) {
        if (v == null) {
            return "null";
        }
        if (v instanceof BigDecimal bd) {
            return bd.toPlainString();
        }
        if (v instanceof Timestamp ts) {
            v = ts.toLocalDateTime();
        }
        if (v instanceof LocalDateTime ldt) {
            return ldt.getNano() == 0 ? ldt.format(TS) : ldt.format(TS_MILLIS);
        }
        return v.toString();
    }
}
