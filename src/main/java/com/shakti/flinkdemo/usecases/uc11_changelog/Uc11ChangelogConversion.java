package com.shakti.flinkdemo.usecases.uc11_changelog;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.H2ChangelogSink;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.model.Order;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * UC-11: DataStream to Table and back, and changelog (retraction) semantics.
 *
 * <p>A DataStream of {@link Order} records becomes a Table ({@code fromDataStream}); a Table API
 * (not SQL) group-by computes totals per customer. A continuous aggregate updates earlier results,
 * so {@code toChangelogStream} emits +I (insert), -U (retract old value) and +U (new value) rows.
 * Every change is logged to {@code changelog_demo}; the upsert result is in {@code changelog_final}.
 */
public class Uc11ChangelogConversion implements UseCase {

    public static final String LOG = "changelog_demo";
    public static final String FINAL = "changelog_final";

    public record Change(int seq, String op, String customerId, long orderCount, int totalQuantity, double grossAmount) {}

    @Override
    public String id() {
        return "uc11";
    }

    @Override
    public String title() {
        return "DataStream <-> Table conversion, Table API, changelog (+I/-U/+U) semantics";
    }

    @Override
    public List<String> outputTables() {
        return List.of(LOG, FINAL);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(LOG, "seq INT PRIMARY KEY, op VARCHAR(2), customer_id VARCHAR(10), order_count BIGINT, "
                + "total_quantity INT, gross_amount DECIMAL(12,2)");
        Db.recreate(FINAL, "customer_id VARCHAR(10) PRIMARY KEY, order_count BIGINT, total_quantity INT, "
                + "gross_amount DECIMAL(12,2)");

        StreamExecutionEnvironment env = Envs.local(1);
        StreamTableEnvironment tEnv = Envs.table(env);

        DataStream<Order> orders = Sources.lines(env, DataFiles.ORDERS)
                .flatMap((String line, Collector<Order> out) -> {
                    try {
                        Order o = Order.parse(line);
                        if (o.quantity() > 0) {
                            out.collect(o);
                        }
                    } catch (IllegalArgumentException malformed) {
                        // dropped
                    }
                })
                .returns(Order.class)
                .name("parse-orders");

        // DataStream -> Table: record fields become columns
        Table ordersTable = tEnv.fromDataStream(orders);

        Table totals = ordersTable
                .groupBy($("customerId"))
                .select(
                        $("customerId"),
                        $("orderId").count().as("orderCount"),
                        $("quantity").sum().as("totalQuantity"),
                        $("quantity").times($("price")).sum().as("grossAmount"));

        // Table -> DataStream: an updating table becomes a changelog stream of Rows with a RowKind
        DataStream<Row> changelog = tEnv.toChangelogStream(totals);

        changelog.map(new RichMapFunction<Row, Change>() {
                    private int seq;

                    @Override
                    public Change map(Row row) {
                        return new Change(++seq, row.getKind().shortString(), (String) row.getField("customerId"),
                                (Long) row.getField("orderCount"), (Integer) row.getField("totalQuantity"),
                                (Double) row.getField("grossAmount"));
                    }
                })
                .name("number-changes")
                .sinkTo(JdbcSinks.sink("INSERT INTO " + LOG + " VALUES (?, ?, ?, ?, ?, ?)", (ps, c) -> {
                    ps.setInt(1, c.seq());
                    ps.setString(2, c.op());
                    ps.setString(3, c.customerId());
                    ps.setLong(4, c.orderCount());
                    ps.setInt(5, c.totalQuantity());
                    ps.setBigDecimal(6, BigDecimal.valueOf(c.grossAmount()));
                }))
                .name("h2:" + LOG);

        changelog.sinkTo(new H2ChangelogSink(FINAL,
                        List.of("customer_id", "order_count", "total_quantity", "gross_amount"),
                        List.of("customer_id")))
                .name("h2:" + FINAL);

        env.execute("uc11-changelog-conversion");
    }
}
