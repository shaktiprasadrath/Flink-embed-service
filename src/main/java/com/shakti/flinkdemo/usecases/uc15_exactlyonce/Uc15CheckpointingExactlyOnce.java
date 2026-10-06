package com.shakti.flinkdemo.usecases.uc15_exactlyonce;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.model.Order;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * UC-15: Checkpointing, restart strategy and exactly-once state.
 *
 * <p>Checkpoints are taken every 100 ms. Processing the 6th order throws once (failure injection).
 * The fixed-delay restart strategy restarts the job; Flink restores operator state and the file
 * source's read position from the last checkpoint and replays the records after it.
 * <ul>
 *   <li>Flink state (per-customer totals) is exactly-once: replayed records are not counted twice.</li>
 *   <li>The JDBC sink is at-least-once. With an idempotent MERGE (exactly_once_*) the DB result is
 *       still exactly-once; a plain INSERT log (at_least_once_log) may contain replayed duplicates.
 *       For non-idempotent writes, the JDBC connector also offers an XA two-phase-commit sink
 *       ({@code JdbcSink.builder().buildExactlyOnce(...)}).</li>
 * </ul>
 */
public class Uc15CheckpointingExactlyOnce implements UseCase {

    public static final String ORDERS = "exactly_once_orders";
    public static final String TOTALS = "exactly_once_totals";
    public static final String LOG = "at_least_once_log";
    public static final String STATS = "exactly_once_stats";

    static final int FAIL_AT_RECORD = 6;

    /** Static: survives the task restart inside this JVM, so the failure happens only once. */
    static final AtomicBoolean FAILED = new AtomicBoolean();
    static final AtomicInteger INVOCATIONS = new AtomicInteger();

    public record CustomerTotal(String customerId, int orderCount, int totalQuantity, double grossAmount) {}

    @Override
    public String id() {
        return "uc15";
    }

    @Override
    public String title() {
        return "Checkpointing + restart strategy + failure recovery with exactly-once state";
    }

    @Override
    public List<String> outputTables() {
        return List.of(ORDERS, TOTALS, LOG, STATS);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(ORDERS, "order_id VARCHAR(10) PRIMARY KEY, customer_id VARCHAR(10), quantity INT");
        Db.recreate(TOTALS, "customer_id VARCHAR(10) PRIMARY KEY, order_count INT, total_quantity INT, "
                + "gross_amount DECIMAL(12,2)");
        Db.recreate(LOG, "order_id VARCHAR(10), customer_id VARCHAR(10)");
        Db.recreate(STATS, "injected_failures INT, records_processed_including_replays INT, distinct_orders INT");
        FAILED.set(false);
        INVOCATIONS.set(0);

        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(200));

        StreamExecutionEnvironment env = Envs.local(2, conf);
        env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);

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
                .name("parse-orders")
                .uid("parse-orders")
                .setParallelism(1)
                .map(Uc15CheckpointingExactlyOnce::slowAndFailOnce)
                .name("failure-injector")
                .uid("failure-injector")
                .setParallelism(1);

        orders.sinkTo(JdbcSinks.sink("MERGE INTO " + ORDERS + " KEY (order_id) VALUES (?, ?, ?)", (ps, o) -> {
            ps.setString(1, o.orderId());
            ps.setString(2, o.customerId());
            ps.setInt(3, o.quantity());
        })).name("h2:" + ORDERS);

        orders.sinkTo(JdbcSinks.sink("INSERT INTO " + LOG + " VALUES (?, ?)", (ps, o) -> {
            ps.setString(1, o.orderId());
            ps.setString(2, o.customerId());
        }, 1, 0)).name("h2:" + LOG);

        orders.keyBy(Order::customerId)
                .process(new TotalsInState())
                .name("totals-in-state")
                .uid("totals-in-state")
                .sinkTo(JdbcSinks.sink("MERGE INTO " + TOTALS + " KEY (customer_id) VALUES (?, ?, ?, ?)", (ps, t) -> {
                    ps.setString(1, t.customerId());
                    ps.setInt(2, t.orderCount());
                    ps.setInt(3, t.totalQuantity());
                    ps.setBigDecimal(4, BigDecimal.valueOf(t.grossAmount()));
                }))
                .name("h2:" + TOTALS);

        env.execute("uc15-checkpointing-exactly-once");

        Db.execute("INSERT INTO " + STATS + " VALUES (" + (FAILED.get() ? 1 : 0) + ", " + INVOCATIONS.get()
                + ", (SELECT COUNT(*) FROM " + ORDERS + "))");
    }

    static Order slowAndFailOnce(Order order) throws InterruptedException {
        Thread.sleep(100); // slow enough that checkpoints complete between records
        if (INVOCATIONS.incrementAndGet() == FAIL_AT_RECORD && FAILED.compareAndSet(false, true)) {
            throw new IllegalStateException("Injected failure while processing " + order.orderId());
        }
        return order;
    }

    /** Totals kept in checkpointed keyed state. */
    static class TotalsInState extends KeyedProcessFunction<String, Order, CustomerTotal> {
        private transient ValueState<CustomerTotal> total;

        @Override
        public void open(OpenContext openContext) {
            total = getRuntimeContext().getState(new ValueStateDescriptor<>("total", CustomerTotal.class));
        }

        @Override
        public void processElement(Order o, Context ctx, Collector<CustomerTotal> out) throws Exception {
            CustomerTotal t = total.value();
            CustomerTotal next = t == null
                    ? new CustomerTotal(o.customerId(), 1, o.quantity(), o.amount())
                    : new CustomerTotal(o.customerId(), t.orderCount() + 1, t.totalQuantity() + o.quantity(),
                    t.grossAmount() + o.amount());
            total.update(next);
            out.collect(next);
        }
    }
}
