package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc15_exactlyonce.Uc15CheckpointingExactlyOnce;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc15CheckpointingExactlyOnceTest {

    @BeforeAll
    static void runOnce() throws Exception {
        new Uc15CheckpointingExactlyOnce().run();
    }

    @Test
    void jobFailedOnceAndReplayedRecords() {
        String[] stats = Db.rows("SELECT * FROM exactly_once_stats").get(0).split("\\|");
        assertThat(stats[0]).isEqualTo("1");                       // one injected failure
        assertThat(Integer.parseInt(stats[1])).isGreaterThan(10);  // the failed record (at least) was replayed
        assertThat(stats[2]).isEqualTo("10");
    }

    @Test
    void stateAndIdempotentSinkAreExactlyOnce() {
        assertThat(Db.count("exactly_once_orders")).isEqualTo(10);
        // same totals as UC-01: replayed records were not counted twice in Flink state
        assertThat(Db.rows("SELECT * FROM exactly_once_totals ORDER BY customer_id")).containsExactly(
                "C1|4|7|2475.00",
                "C2|3|6|1290.00",
                "C3|3|6|380.00");
    }

    @Test
    void plainInsertLogIsAtLeastOnce() {
        assertThat(Db.count("at_least_once_log")).isGreaterThanOrEqualTo(10);
        assertThat(Db.rows("SELECT COUNT(DISTINCT order_id) FROM at_least_once_log")).containsExactly("10");
    }
}
