package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc07_sideoutputs.Uc07SideOutputs;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc07SideOutputsTest {

    @Test
    void splitsValidOrdersAndDeadLetters() throws Exception {
        new Uc07SideOutputs().run();

        assertThat(Db.count("orders_valid")).isEqualTo(10);
        assertThat(Db.rows("SELECT * FROM orders_dead_letter ORDER BY raw_line")).containsExactly(
                "O11,C2,keyboard,-1,80.00,USD,2026-01-02 16:00:00|non-positive quantity",
                "this,is,not,valid|parse error: expected 7 fields but got 4");
    }
}
