package com.shakti.flinkdemo.usecases.uc09_sql;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.H2ChangelogSink;
import com.shakti.flinkdemo.common.TableDdl;
import com.shakti.flinkdemo.common.UseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

/**
 * UC-09: Flink SQL on files.
 * <ul>
 *   <li>DDL: filesystem connector, csv/json formats, computed column, watermark</li>
 *   <li>Window table-valued function: TUMBLE daily revenue</li>
 *   <li>Top-N: the 2 products with the highest quantity (ROW_NUMBER over an aggregate)</li>
 *   <li>Deduplication: first page each user visited (ROW_NUMBER ... = 1, by event time)</li>
 * </ul>
 * Query results are changelog streams (Top-N and dedup update earlier results), so they are
 * written with an upsert sink.
 */
public class Uc09TableApiSql implements UseCase {

    public static final String DAILY_SALES = "sql_daily_sales";
    public static final String TOP_PRODUCTS = "sql_top_products";
    public static final String FIRST_PAGE = "sql_first_page";

    @Override
    public String id() {
        return "uc09";
    }

    @Override
    public String title() {
        return "Flink SQL: DDL, window TVF, Top-N, deduplication";
    }

    @Override
    public List<String> outputTables() {
        return List.of(DAILY_SALES, TOP_PRODUCTS, FIRST_PAGE);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(DAILY_SALES, "window_start TIMESTAMP PRIMARY KEY, window_end TIMESTAMP, orders BIGINT, "
                + "revenue DECIMAL(12,2)");
        Db.recreate(TOP_PRODUCTS, "product_rank BIGINT PRIMARY KEY, product VARCHAR(20), total_quantity INT");
        Db.recreate(FIRST_PAGE, "user_id VARCHAR(10) PRIMARY KEY, page VARCHAR(40), ts TIMESTAMP");

        StreamExecutionEnvironment env = Envs.local(1);
        StreamTableEnvironment tEnv = Envs.table(env);
        TableDdl.orders(tEnv);
        TableDdl.clicks(tEnv);

        Table dailySales = tEnv.sqlQuery("""
                SELECT window_start, window_end, COUNT(*) AS orders, SUM(quantity * price) AS revenue
                FROM TABLE(TUMBLE(TABLE valid_orders, DESCRIPTOR(order_time), INTERVAL '1' DAY))
                GROUP BY window_start, window_end""");

        Table topProducts = tEnv.sqlQuery("""
                SELECT product_rank, product, total_quantity
                FROM (
                  SELECT product, total_quantity,
                         ROW_NUMBER() OVER (ORDER BY total_quantity DESC) AS product_rank
                  FROM (SELECT product, SUM(quantity) AS total_quantity FROM valid_orders GROUP BY product)
                )
                WHERE product_rank <= 2""");

        Table firstPage = tEnv.sqlQuery("""
                SELECT user_id, page, ts
                FROM (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts ASC) AS rn
                  FROM clicks
                )
                WHERE rn = 1""");

        tEnv.toChangelogStream(dailySales)
                .sinkTo(new H2ChangelogSink(DAILY_SALES, List.of("window_start", "window_end", "orders", "revenue"),
                        List.of("window_start")))
                .name("h2:" + DAILY_SALES);
        tEnv.toChangelogStream(topProducts)
                .sinkTo(new H2ChangelogSink(TOP_PRODUCTS, List.of("product_rank", "product", "total_quantity"),
                        List.of("product_rank")))
                .name("h2:" + TOP_PRODUCTS);
        tEnv.toChangelogStream(firstPage)
                .sinkTo(new H2ChangelogSink(FIRST_PAGE, List.of("user_id", "page", "ts"), List.of("user_id")))
                .name("h2:" + FIRST_PAGE);

        // all three queries run in one Flink job
        env.execute("uc09-table-api-sql");
    }
}
