package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc14_async.Uc14AsyncIo;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc14AsyncIoTest {

    @Test
    void enrichesAsynchronouslyInInputOrderWithRetries() throws Exception {
        new Uc14AsyncIo().run();

        // seq = output order: same as input order (orderedWait), although T7's lookup finished first
        assertThat(Db.rows("SELECT * FROM async_enriched ORDER BY seq")).containsExactly(
                "1|T1|A1|120.00|LOW|1",
                "2|T2|A2|900.00|MEDIUM|2",
                "3|T3|A1|50.00|LOW|1",
                "4|T4|A3|2500.00|HIGH|1",
                "5|T5|A2|30.00|MEDIUM|2",
                "6|T6|A3|75.00|HIGH|1",
                "7|T7|A1|600.00|LOW|1");
    }
}
