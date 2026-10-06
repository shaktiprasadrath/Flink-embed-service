package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc01_basics.Uc01BasicTransformations;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc01BasicTransformationsTest {

    @Test
    void aggregatesValidOrdersPerCustomer() throws Exception {
        new Uc01BasicTransformations().run();

        // O11 (negative quantity) and the malformed line are dropped.
        assertThat(Db.rows("SELECT customer_id, order_count, total_quantity, gross_amount "
                + "FROM customer_totals ORDER BY customer_id"))
                .containsExactly(
                        "C1|4|7|2475.00",
                        "C2|3|6|1290.00",
                        "C3|3|6|380.00");
    }
}
