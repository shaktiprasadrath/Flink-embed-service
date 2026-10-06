package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc13_cep.Uc13ComplexEventProcessing;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc13ComplexEventProcessingTest {

    @Test
    void detectsBruteForcePatternAndTimeouts() throws Exception {
        new Uc13ComplexEventProcessing().run();

        assertThat(Db.rows("SELECT user_id, FORMATDATETIME(first_fail, 'HH:mm:ss'), "
                + "FORMATDATETIME(success_time, 'HH:mm:ss'), fail_count, status FROM cep_matches ORDER BY user_id"))
                .containsExactly(
                        "alice|10:00:00|10:00:30|3|MATCH",
                        "carol|10:02:10|10:02:40|3|MATCH",   // the first of carol's 4 failures is not part of it
                        "dave|10:03:00|null|3|TIMEOUT");     // success came 17 min later, outside 5 min
    }
}
