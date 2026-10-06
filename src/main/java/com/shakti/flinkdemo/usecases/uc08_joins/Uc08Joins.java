package com.shakti.flinkdemo.usecases.uc08_joins;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.common.Watermarks;
import com.shakti.flinkdemo.model.Order;
import com.shakti.flinkdemo.model.Payment;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;

import java.sql.Types;
import java.time.Duration;
import java.util.List;

/**
 * UC-08: Joining two streams (orders and payments, keyed by order id) three ways:
 * <ul>
 *   <li>INTERVAL - payment within [order_time, order_time + 30 min]</li>
 *   <li>WINDOW   - order and payment fall in the same 1-hour tumbling window</li>
 *   <li>CO_PROCESS - connect + KeyedCoProcessFunction: custom join logic with state and timers;
 *       matches at any time, and reports UNPAID orders / ORPHAN payments after 24 h</li>
 * </ul>
 */
public class Uc08Joins implements UseCase {

    public static final String TABLE = "join_results";
    static final long UNMATCHED_TIMEOUT = Duration.ofHours(24).toMillis();

    public record JoinResult(String joinType, String orderId, String paymentId, String status) {}

    @Override
    public String id() {
        return "uc08";
    }

    @Override
    public String title() {
        return "Joins: interval join, window join, connect + CoProcessFunction";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TABLE);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(TABLE, "join_type VARCHAR(12), order_id VARCHAR(10), payment_id VARCHAR(10), status VARCHAR(16)");

        StreamExecutionEnvironment env = Envs.local(2);

        DataStream<Order> orders = Sources.lines(env, DataFiles.ORDERS)
                .flatMap(Uc08Joins::parseValid).returns(Order.class).name("valid-orders").setParallelism(1)
                .assignTimestampsAndWatermarks(
                        Watermarks.<Order>perEvent(Duration.ofMinutes(1), (o, ts) -> o.orderTime()))
                .name("watermarks:orders").setParallelism(1);
        DataStream<Payment> payments = Sources
                .events(env, DataFiles.PAYMENTS, Payment::parse, Payment.class,
                        Watermarks.<Payment>perEvent(Duration.ofMinutes(1), (p, ts) -> p.paymentTime()));

        DataStream<JoinResult> interval = orders.keyBy(Order::orderId)
                .intervalJoin(payments.keyBy(Payment::orderId))
                .between(Duration.ZERO, Duration.ofMinutes(30))
                .process(new ProcessJoinFunction<Order, Payment, JoinResult>() {
                    @Override
                    public void processElement(Order o, Payment p, Context ctx, Collector<JoinResult> out) {
                        out.collect(new JoinResult("INTERVAL", o.orderId(), p.paymentId(), "MATCH"));
                    }
                })
                .name("interval-join");

        DataStream<JoinResult> window = orders.join(payments)
                .where(Order::orderId)
                .equalTo(Payment::orderId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .apply((o, p) -> new JoinResult("WINDOW", o.orderId(), p.paymentId(), "MATCH"),
                        TypeInformation.of(JoinResult.class));

        DataStream<JoinResult> coProcess = orders.connect(payments)
                .keyBy(Order::orderId, Payment::orderId)
                .process(new OrderPaymentMatcher())
                .name("co-process-join");

        interval.union(window, coProcess)
                .sinkTo(JdbcSinks.sink("INSERT INTO " + TABLE + " VALUES (?, ?, ?, ?)", (ps, r) -> {
                    ps.setString(1, r.joinType());
                    ps.setString(2, r.orderId());
                    if (r.paymentId() == null) {
                        ps.setNull(3, Types.VARCHAR);
                    } else {
                        ps.setString(3, r.paymentId());
                    }
                    ps.setString(4, r.status());
                }))
                .name("h2:" + TABLE);

        env.execute("uc08-joins");
    }

    /** Emits only well-formed orders with a positive quantity (see UC-07 for dead-lettering the rest). */
    static void parseValid(String line, Collector<Order> out) {
        try {
            Order order = Order.parse(line);
            if (order.quantity() > 0) {
                out.collect(order);
            }
        } catch (IllegalArgumentException malformed) {
            // dropped
        }
    }

    static class OrderPaymentMatcher extends KeyedCoProcessFunction<String, Order, Payment, JoinResult> {
        private transient ValueState<Order> pendingOrder;
        private transient ValueState<Payment> pendingPayment;

        @Override
        public void open(OpenContext openContext) {
            pendingOrder = getRuntimeContext().getState(new ValueStateDescriptor<>("order", Order.class));
            pendingPayment = getRuntimeContext().getState(new ValueStateDescriptor<>("payment", Payment.class));
        }

        @Override
        public void processElement1(Order order, Context ctx, Collector<JoinResult> out) throws Exception {
            Payment payment = pendingPayment.value();
            if (payment != null) {
                out.collect(new JoinResult("CO_PROCESS", order.orderId(), payment.paymentId(), "MATCH"));
                pendingPayment.clear();
            } else {
                pendingOrder.update(order);
                ctx.timerService().registerEventTimeTimer(order.orderTime() + UNMATCHED_TIMEOUT);
            }
        }

        @Override
        public void processElement2(Payment payment, Context ctx, Collector<JoinResult> out) throws Exception {
            Order order = pendingOrder.value();
            if (order != null) {
                out.collect(new JoinResult("CO_PROCESS", order.orderId(), payment.paymentId(), "MATCH"));
                pendingOrder.clear();
            } else {
                pendingPayment.update(payment);
                ctx.timerService().registerEventTimeTimer(payment.paymentTime() + UNMATCHED_TIMEOUT);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<JoinResult> out) throws Exception {
            // state was cleared on a match, so whatever is still pending never got a partner
            if (pendingOrder.value() != null) {
                out.collect(new JoinResult("CO_PROCESS", ctx.getCurrentKey(), null, "UNPAID"));
                pendingOrder.clear();
            }
            if (pendingPayment.value() != null) {
                out.collect(new JoinResult("CO_PROCESS", ctx.getCurrentKey(), pendingPayment.value().paymentId(),
                        "ORPHAN_PAYMENT"));
                pendingPayment.clear();
            }
        }
    }
}
