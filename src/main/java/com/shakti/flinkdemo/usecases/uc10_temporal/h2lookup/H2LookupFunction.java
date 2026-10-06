package com.shakti.flinkdemo.usecases.uc10_temporal.h2lookup;

import com.shakti.flinkdemo.common.Db;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** Runtime part of the h2-lookup connector: SELECT ... WHERE key = ? for each probe row. */
public class H2LookupFunction extends LookupFunction {

    private final String tableName;
    private final List<String> columns;
    private final List<LogicalType> types;
    private final List<String> keyColumns;
    private final List<LogicalType> keyTypes;

    private transient Connection connection;
    private transient PreparedStatement query;

    public H2LookupFunction(String tableName, List<String> columns, List<LogicalType> types,
                            List<String> keyColumns, List<LogicalType> keyTypes) {
        this.tableName = tableName;
        this.columns = List.copyOf(columns);
        this.types = List.copyOf(types);
        this.keyColumns = List.copyOf(keyColumns);
        this.keyTypes = List.copyOf(keyTypes);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        connection = Db.connect();
        query = connection.prepareStatement("SELECT " + String.join(", ", columns) + " FROM " + tableName
                + " WHERE " + keyColumns.stream().map(c -> c + " = ?").collect(Collectors.joining(" AND ")));
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        try {
            for (int i = 0; i < keyColumns.size(); i++) {
                Object key = RowData.createFieldGetter(keyTypes.get(i), i).getFieldOrNull(keyRow);
                query.setObject(i + 1, toJdbc(key));
            }
            List<RowData> rows = new ArrayList<>();
            try (ResultSet rs = query.executeQuery()) {
                while (rs.next()) {
                    GenericRowData row = new GenericRowData(columns.size());
                    for (int i = 0; i < columns.size(); i++) {
                        row.setField(i, toInternal(rs, i + 1, types.get(i)));
                    }
                    rows.add(row);
                }
            }
            return rows;
        } catch (SQLException e) {
            throw new IOException("Lookup in " + tableName + " failed", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (query != null) {
            query.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    private static Object toJdbc(Object internal) {
        if (internal instanceof StringData s) {
            return s.toString();
        }
        if (internal instanceof DecimalData d) {
            return d.toBigDecimal();
        }
        return internal;
    }

    /** Converts a JDBC value to Flink's internal data structure for the column type. */
    private static Object toInternal(ResultSet rs, int index, LogicalType type) throws SQLException {
        Object value = rs.getObject(index);
        if (value == null) {
            return null;
        }
        return switch (type.getTypeRoot()) {
            case CHAR, VARCHAR -> StringData.fromString(value.toString());
            case INTEGER -> rs.getInt(index);
            case BIGINT -> rs.getLong(index);
            case DOUBLE -> rs.getDouble(index);
            case BOOLEAN -> rs.getBoolean(index);
            case DECIMAL -> {
                DecimalType decimal = (DecimalType) type;
                BigDecimal bd = rs.getBigDecimal(index);
                yield DecimalData.fromBigDecimal(bd, decimal.getPrecision(), decimal.getScale());
            }
            default -> throw new UnsupportedOperationException("h2-lookup does not support type " + type);
        };
    }
}
