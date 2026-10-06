package com.shakti.flinkdemo.usecases.uc07_sideoutputs;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.Times;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.model.Order;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.util.List;

/**
 * UC-07: Side outputs. One ProcessFunction splits the input into the main output (valid orders)
 * and a side output (dead letters with the reason), without filtering the stream twice.
 * UC-03 shows the built-in side output for late window data.
 */
public class Uc07SideOutputs implements UseCase {

    public static final String VALID = "orders_valid";
    public static final String DEAD_LETTER = "orders_dead_letter";

    public record DeadLetter(String rawLine, String reason) {}

    static final OutputTag<DeadLetter> DEAD_LETTERS = new OutputTag<>("dead-letters") {};

    @Override
    public String id() {
        return "uc07";
    }

    @Override
    public String title() {
        return "Side outputs: route invalid records to a dead-letter table";
    }

    @Override
    public List<String> outputTables() {
        return List.of(VALID, DEAD_LETTER);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(VALID, "order_id VARCHAR(10) PRIMARY KEY, customer_id VARCHAR(10), product VARCHAR(20), "
                + "quantity INT, price DECIMAL(12,2), currency VARCHAR(3), order_time TIMESTAMP");
        Db.recreate(DEAD_LETTER, "raw_line VARCHAR(200), reason VARCHAR(200)");

        StreamExecutionEnvironment env = Envs.local(2);

        SingleOutputStreamOperator<Order> valid = Sources.lines(env, DataFiles.ORDERS)
                .process(new ProcessFunction<String, Order>() {
                    @Override
                    public void processElement(String line, Context ctx, Collector<Order> out) {
                        Order order;
                        try {
                            order = Order.parse(line);
                        } catch (IllegalArgumentException e) {
                            ctx.output(DEAD_LETTERS, new DeadLetter(line, "parse error: " + e.getMessage()));
                            return;
                        }
                        if (order.quantity() <= 0) {
                            ctx.output(DEAD_LETTERS, new DeadLetter(line, "non-positive quantity"));
                        } else {
                            out.collect(order);
                        }
                    }
                })
                .name("validate-orders");

        valid.sinkTo(JdbcSinks.sink("INSERT INTO " + VALID + " VALUES (?, ?, ?, ?, ?, ?, ?)", (ps, o) -> {
            ps.setString(1, o.orderId());
            ps.setString(2, o.customerId());
            ps.setString(3, o.product());
            ps.setInt(4, o.quantity());
            ps.setBigDecimal(5, BigDecimal.valueOf(o.price()));
            ps.setString(6, o.currency());
            ps.setTimestamp(7, Times.sql(o.orderTime()));
        })).name("h2:" + VALID);

        valid.getSideOutput(DEAD_LETTERS)
                .sinkTo(JdbcSinks.sink("INSERT INTO " + DEAD_LETTER + " VALUES (?, ?)", (ps, d) -> {
                    ps.setString(1, d.rawLine());
                    ps.setString(2, d.reason());
                }))
                .name("h2:" + DEAD_LETTER);

        env.execute("uc07-side-outputs");
    }
}
