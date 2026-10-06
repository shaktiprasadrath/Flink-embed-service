package com.shakti.flinkdemo.usecases.uc10_temporal.h2lookup;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.Set;

/**
 * Custom Table connector: {@code 'connector' = 'h2-lookup'}. Found by Flink through
 * META-INF/services/org.apache.flink.table.factories.Factory.
 *
 * <p>The official JDBC connector has no H2 dialect, so this ~150-line connector shows the Table
 * connector SPI (factory -> DynamicTableSource -> runtime LookupFunction).
 */
public class H2LookupTableFactory implements DynamicTableSourceFactory {

    public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name")
            .stringType()
            .noDefaultValue()
            .withDescription("Name of the H2 table to look up rows in.");

    @Override
    public String factoryIdentifier() {
        return "h2-lookup";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(TABLE_NAME);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        DataType rowType = context.getPhysicalRowDataType();
        return new H2LookupTableSource(helper.getOptions().get(TABLE_NAME), rowType);
    }
}
