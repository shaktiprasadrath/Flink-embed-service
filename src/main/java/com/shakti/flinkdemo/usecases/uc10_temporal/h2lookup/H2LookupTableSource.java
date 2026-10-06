package com.shakti.flinkdemo.usecases.uc10_temporal.h2lookup;

import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/** Lookup-only table source: Flink calls it once per probe row of a lookup join. */
public class H2LookupTableSource implements LookupTableSource {

    private final String tableName;
    private final DataType rowType;

    public H2LookupTableSource(String tableName, DataType rowType) {
        this.tableName = tableName;
        this.rowType = rowType;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        RowType row = (RowType) rowType.getLogicalType();
        List<String> columns = row.getFieldNames();
        List<LogicalType> types = row.getChildren();

        // keys = the columns used in the join's ON clause; each entry is a (possibly nested) field path
        List<String> keyColumns = new ArrayList<>();
        List<LogicalType> keyTypes = new ArrayList<>();
        for (int[] path : context.getKeys()) {
            keyColumns.add(columns.get(path[0]));
            keyTypes.add(types.get(path[0]));
        }
        return LookupFunctionProvider.of(new H2LookupFunction(tableName, columns, types, keyColumns, keyTypes));
    }

    @Override
    public DynamicTableSource copy() {
        return new H2LookupTableSource(tableName, rowType);
    }

    @Override
    public String asSummaryString() {
        return "H2Lookup(" + tableName + ")";
    }
}
