package com.shakti.flinkdemo.usecases.uc17_batch;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.model.Order;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * UC-17: The same DataStream program in STREAMING and BATCH execution mode.
 *
 * <p>Total quantity per product with keyBy + reduce. In STREAMING mode the reduce emits an updated
 * total for every order (10 rows). In BATCH mode Flink knows the input is bounded, sorts by key and
 * emits only the final total per key (4 rows). The final totals are identical.
 */
public class Uc17BatchExecutionMode implements UseCase {

    public static final String TABLE = "batch_vs_stream";

    public record ProductQuantity(String product, int quantity) {}

    @Override
    public String id() {
        return "uc17";
    }

    @Override
    public String title() {
        return "Execution modes: same job in STREAMING vs BATCH";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TABLE);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(TABLE, "execution_mode VARCHAR(10), product VARCHAR(20), total_quantity INT");
        runIn(RuntimeExecutionMode.STREAMING);
        runIn(RuntimeExecutionMode.BATCH);
    }

    private static void runIn(RuntimeExecutionMode mode) throws Exception {
        StreamExecutionEnvironment env = Envs.local(2);
        env.setRuntimeMode(mode);

        Sources.lines(env, DataFiles.ORDERS)
                .flatMap((String line, Collector<ProductQuantity> out) -> {
                    try {
                        Order o = Order.parse(line);
                        if (o.quantity() > 0) {
                            out.collect(new ProductQuantity(o.product(), o.quantity()));
                        }
                    } catch (IllegalArgumentException malformed) {
                        // dropped
                    }
                })
                .returns(ProductQuantity.class)
                .name("parse-orders")
                .keyBy(ProductQuantity::product)
                .reduce((a, b) -> new ProductQuantity(a.product(), a.quantity() + b.quantity()))
                .name("sum-per-product")
                .sinkTo(JdbcSinks.sink("INSERT INTO " + TABLE + " VALUES (?, ?, ?)", (ps, p) -> {
                    ps.setString(1, mode.name());
                    ps.setString(2, p.product());
                    ps.setInt(3, p.quantity());
                }))
                .name("h2:" + TABLE);

        env.execute("uc17-" + mode.name().toLowerCase());
    }
}
