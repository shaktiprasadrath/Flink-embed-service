package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc16_savepoints.Uc16SavepointsStateBackend;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc16SavepointsStateBackendTest {

    @Test
    void resumesCountsFromSavepoint() throws Exception {
        Uc16SavepointsStateBackend uc = new Uc16SavepointsStateBackend();
        uc.run();

        assertThat(uc.lastSavepoint()).contains("savepoint-");
        // part 1: u1=3, u2=2, u3=1; part 2 adds 2 each. Phase 2 continued from the restored counts.
        assertThat(Db.rows("SELECT * FROM savepoint_demo ORDER BY user_id")).containsExactly(
                "u1|5|2",
                "u2|4|2",
                "u3|3|2");
    }
}
