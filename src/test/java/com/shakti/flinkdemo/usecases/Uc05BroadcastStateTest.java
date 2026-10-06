package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc05_broadcast.Uc05BroadcastState;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc05BroadcastStateTest {

    @Test
    void flagsTransactionsMatchingBroadcastRules() throws Exception {
        new Uc05BroadcastState().run();

        // R1: amount > 500, R2: merchant = casino
        assertThat(Db.rows("SELECT * FROM flagged_txns ORDER BY txn_id, rule_id")).containsExactly(
                "T2|A2|900.00|R1",
                "T3|A1|50.00|R2",
                "T4|A3|2500.00|R1",
                "T4|A3|2500.00|R2",
                "T7|A1|600.00|R1");
    }
}
