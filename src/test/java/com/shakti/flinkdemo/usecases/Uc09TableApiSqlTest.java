package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc09_sql.Uc09TableApiSql;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc09TableApiSqlTest {

    @BeforeAll
    static void runOnce() throws Exception {
        new Uc09TableApiSql().run();
    }

    @Test
    void dailyRevenueFromTumbleWindowTvf() {
        assertThat(Db.rows("SELECT * FROM sql_daily_sales ORDER BY window_start")).containsExactly(
                "2026-01-01 00:00:00|2026-01-02 00:00:00|6|2605.00",
                "2026-01-02 00:00:00|2026-01-03 00:00:00|4|1540.00");
    }

    @Test
    void topTwoProductsByQuantity() {
        assertThat(Db.rows("SELECT * FROM sql_top_products ORDER BY product_rank"))
                .containsExactly("1|mouse|9", "2|keyboard|4");
    }

    @Test
    void firstPagePerUserViaDeduplication() {
        assertThat(Db.rows("SELECT * FROM sql_first_page ORDER BY user_id")).containsExactly(
                "u1|/home|2026-01-01 10:00:00",
                "u2|/home|2026-01-01 10:00:30",
                "u3|/home|2026-01-01 10:01:15");
    }
}
