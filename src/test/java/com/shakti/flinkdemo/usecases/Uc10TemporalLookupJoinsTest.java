package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc10_temporal.Uc10TemporalLookupJoins;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc10TemporalLookupJoinsTest {

    @Test
    void enrichesOrdersWithCustomerAndRateValidAtOrderTime() throws Exception {
        new Uc10TemporalLookupJoins().run();

        // EUR is 1.10 on 2026-01-01 (O3, O5) and 1.20 from 2026-01-02 (O10)
        assertThat(Db.rows("SELECT * FROM orders_enriched ORDER BY CAST(SUBSTRING(order_id, 2) AS INT)"))
                .containsExactly(
                        "O1|Acme Corp|GOLD|1000.00|USD|1.0000|1000.00",
                        "O2|Globex|SILVER|50.00|USD|1.0000|50.00",
                        "O3|Acme Corp|GOLD|400.00|EUR|1.1000|440.00",
                        "O4|Initech|BRONZE|80.00|USD|1.0000|80.00",
                        "O5|Globex|SILVER|1000.00|EUR|1.1000|1100.00",
                        "O6|Acme Corp|GOLD|75.00|USD|1.0000|75.00",
                        "O7|Initech|BRONZE|200.00|USD|1.0000|200.00",
                        "O8|Globex|SILVER|240.00|USD|1.0000|240.00",
                        "O9|Acme Corp|GOLD|1000.00|USD|1.0000|1000.00",
                        "O10|Initech|BRONZE|100.00|EUR|1.2000|120.00");
    }
}
