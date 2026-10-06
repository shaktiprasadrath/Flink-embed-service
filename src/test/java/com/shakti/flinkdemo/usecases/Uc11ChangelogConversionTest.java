package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc11_changelog.Uc11ChangelogConversion;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc11ChangelogConversionTest {

    @BeforeAll
    static void runOnce() throws Exception {
        new Uc11ChangelogConversion().run();
    }

    @Test
    void emitsInsertsAndRetractionPairs() {
        // 3 customers -> 3 inserts; the other 7 orders each retract (-U) and replace (+U) a total
        assertThat(Db.rows("SELECT op, COUNT(*) FROM changelog_demo GROUP BY op ORDER BY op"))
                .containsExactly("+I|3", "+U|7", "-U|7");
        assertThat(Db.rows("SELECT op, customer_id, order_count FROM changelog_demo WHERE seq <= 5 ORDER BY seq"))
                .containsExactly("+I|C1|1", "+I|C2|1", "-U|C1|1", "+U|C1|2", "+I|C3|1");
    }

    @Test
    void upsertedResultMatchesFinalTotals() {
        assertThat(Db.rows("SELECT * FROM changelog_final ORDER BY customer_id")).containsExactly(
                "C1|4|7|2475.00",
                "C2|3|6|1290.00",
                "C3|3|6|380.00");
    }
}
