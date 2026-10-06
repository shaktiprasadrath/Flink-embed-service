package com.shakti.flinkdemo.usecases.uc01_basics;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.model.Order;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.List;

/**
 * UC-01: DataStream API basics.
 * flatMap (parse, drop malformed lines) -> filter (positive quantity) -> map (to a per-order total)
 * -> keyBy (customer) -> reduce (running totals) -> JDBC upsert into H2.
 */
public class Uc01BasicTransformations implements UseCase {

    public static final String TABLE = "customer_totals";

    /** Running totals per customer. */
    public record CustomerTotal(String customerId, int orderCount, int totalQuantity, double grossAmount) {}

    @Override
    public String id() {
        return "uc01";
    }

    @Override
    public String title() {
        return "DataStream basics: map, flatMap, filter, keyBy, reduce";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TABLE);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(TABLE, "customer_id VARCHAR(10) PRIMARY KEY, order_count INT, total_quantity INT, "
                + "gross_amount DECIMAL(12,2)");

        StreamExecutionEnvironment env = Envs.local(2);

        DataStream<Order> orders = Sources.lines(env, DataFiles.ORDERS)
                .flatMap((String line, Collector<Order> out) -> {
                    try {
                        out.collect(Order.parse(line));
                    } catch (IllegalArgumentException malformed) {
                        // flatMap may emit zero records: malformed lines are dropped here
                    }
                })
                .returns(Order.class)
                .name("parse-orders")
                .filter(order -> order.quantity() > 0)
                .name("positive-quantity");

        orders.map(o -> new CustomerTotal(o.customerId(), 1, o.quantity(), o.amount()))
                .name("to-customer-total")
                .keyBy(CustomerTotal::customerId)
                .reduce((a, b) -> new CustomerTotal(a.customerId(), a.orderCount() + b.orderCount(),
                        a.totalQuantity() + b.totalQuantity(), a.grossAmount() + b.grossAmount()))
                .name("running-totals")
                // reduce emits an updated total per input record; MERGE keeps only the latest per key
                .sinkTo(JdbcSinks.sink(
                        "MERGE INTO " + TABLE + " (customer_id, order_count, total_quantity, gross_amount) "
                                + "KEY (customer_id) VALUES (?, ?, ?, ?)",
                        (ps, t) -> {
                            ps.setString(1, t.customerId());
                            ps.setInt(2, t.orderCount());
                            ps.setInt(3, t.totalQuantity());
                            ps.setBigDecimal(4, BigDecimal.valueOf(t.grossAmount()));
                        }))
                .name("h2:" + TABLE);

        env.execute("uc01-basic-transformations");
    }
}
