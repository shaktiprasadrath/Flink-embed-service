package com.shakti.flinkdemo.usecases.uc10_temporal;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.H2ChangelogSink;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.TableDdl;
import com.shakti.flinkdemo.common.UseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * UC-10: Enriching a stream in SQL.
 * <ul>
 *   <li>Lookup join ({@code FOR SYSTEM_TIME AS OF o.proc_time}): customer name/tier fetched from the
 *       H2 table {@code customers} at processing time, via the custom {@code h2-lookup} connector.</li>
 *   <li>Event-time temporal join ({@code FOR SYSTEM_TIME AS OF o.order_time}): each order uses the
 *       currency rate that was valid at the order's time. The rates file is append-only, so a
 *       deduplicating view (latest row per currency) turns it into a versioned table.</li>
 * </ul>
 * EUR changes from 1.10 to 1.20 on 2026-01-02, so O3/O5 (day 1) and O10 (day 2) use different rates.
 */
public class Uc10TemporalLookupJoins implements UseCase {

    public static final String TABLE = "orders_enriched";
    public static final String CUSTOMERS = "customers";

    @Override
    public String id() {
        return "uc10";
    }

    @Override
    public String title() {
        return "SQL joins: event-time temporal join (versioned rates) + lookup join (custom connector)";
    }

    @Override
    public List<String> outputTables() {
        return List.of(CUSTOMERS, TABLE);
    }

    @Override
    public void run() throws Exception {
        seedCustomers();
        Db.recreate(TABLE, "order_id VARCHAR(10) PRIMARY KEY, customer_name VARCHAR(40), tier VARCHAR(10), "
                + "amount DECIMAL(12,2), currency VARCHAR(3), rate_to_usd DECIMAL(10,4), amount_usd DECIMAL(12,2)");

        StreamExecutionEnvironment env = Envs.local(1);
        StreamTableEnvironment tEnv = Envs.table(env);
        TableDdl.orders(tEnv);
        TableDdl.rates(tEnv);

        tEnv.executeSql("""
                CREATE TEMPORARY TABLE customers_dim (
                  customer_id STRING,
                  name        STRING,
                  tier        STRING,
                  PRIMARY KEY (customer_id) NOT ENFORCED
                ) WITH (
                  'connector' = 'h2-lookup',
                  'table-name' = 'customers'
                )""");

        // Versioned table: primary key (currency, from the dedup) + event time (valid_from)
        tEnv.executeSql("""
                CREATE TEMPORARY VIEW versioned_rates AS
                SELECT currency, rate_to_usd, valid_from
                FROM (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY valid_from DESC) AS rn
                  FROM rates
                )
                WHERE rn = 1""");

        tEnv.executeSql("""
                CREATE TEMPORARY VIEW orders_with_customer AS
                SELECT o.*, c.name AS customer_name, c.tier
                FROM valid_orders AS o
                JOIN customers_dim FOR SYSTEM_TIME AS OF o.proc_time AS c
                  ON o.customer_id = c.customer_id""");

        Table enriched = tEnv.sqlQuery("""
                SELECT o.order_id, o.customer_name, o.tier,
                       CAST(o.quantity * o.price AS DECIMAL(12, 2)) AS amount,
                       o.currency, r.rate_to_usd,
                       CAST(o.quantity * o.price * r.rate_to_usd AS DECIMAL(12, 2)) AS amount_usd
                FROM orders_with_customer AS o
                JOIN versioned_rates FOR SYSTEM_TIME AS OF o.order_time AS r
                  ON o.currency = r.currency""");

        tEnv.toChangelogStream(enriched)
                .sinkTo(new H2ChangelogSink(TABLE, List.of("order_id", "customer_name", "tier", "amount", "currency",
                        "rate_to_usd", "amount_usd"), List.of("order_id")))
                .name("h2:" + TABLE);

        env.execute("uc10-temporal-lookup-joins");
    }

    /** The lookup side lives in the database: load customers.csv into H2 first. */
    static void seedCustomers() throws IOException {
        Db.recreate(CUSTOMERS, "customer_id VARCHAR(10) PRIMARY KEY, name VARCHAR(40), tier VARCHAR(10)");
        try (Connection c = Db.connect();
             PreparedStatement insert = c.prepareStatement("INSERT INTO " + CUSTOMERS + " VALUES (?, ?, ?)")) {
            for (String line : Files.readAllLines(DataFiles.path(DataFiles.CUSTOMERS))) {
                if (!Sources.isData(line)) {
                    continue;
                }
                String[] f = line.split(",");
                insert.setString(1, f[0].trim());
                insert.setString(2, f[1].trim());
                insert.setString(3, f[2].trim());
                insert.executeUpdate();
            }
        } catch (java.sql.SQLException e) {
            throw new IOException("Cannot seed " + CUSTOMERS, e);
        }
    }
}
