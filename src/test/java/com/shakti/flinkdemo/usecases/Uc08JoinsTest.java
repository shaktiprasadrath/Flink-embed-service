package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc08_joins.Uc08Joins;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class Uc08JoinsTest {

    @BeforeAll
    static void runOnce() throws Exception {
        new Uc08Joins().run();
    }

    private static List<String> rows(String joinType) {
        return Db.rows("SELECT order_id, payment_id, status FROM join_results WHERE join_type = '" + joinType + "'");
    }

    @Test
    void intervalJoinMatchesPaymentsWithin30Minutes() {
        // O2 (paid after 45 min) and O9 (after 40 min) fall outside the interval
        assertThat(rows("INTERVAL")).containsExactlyInAnyOrder(
                "O1|P1|MATCH", "O3|P3|MATCH", "O5|P4|MATCH", "O7|P5|MATCH", "O10|P7|MATCH");
    }

    @Test
    void windowJoinMatchesWithinSameHour() {
        assertThat(rows("WINDOW")).containsExactlyInAnyOrder(
                "O1|P1|MATCH", "O2|P2|MATCH", "O3|P3|MATCH", "O5|P4|MATCH", "O7|P5|MATCH", "O9|P6|MATCH",
                "O10|P7|MATCH");
    }

    @Test
    void coProcessJoinReportsMatchesUnpaidOrdersAndOrphanPayments() {
        assertThat(rows("CO_PROCESS")).containsExactlyInAnyOrder(
                "O1|P1|MATCH", "O2|P2|MATCH", "O3|P3|MATCH", "O5|P4|MATCH", "O7|P5|MATCH", "O9|P6|MATCH",
                "O10|P7|MATCH",
                "O4|null|UNPAID", "O6|null|UNPAID", "O8|null|UNPAID",
                "O99|P8|ORPHAN_PAYMENT");
    }
}
