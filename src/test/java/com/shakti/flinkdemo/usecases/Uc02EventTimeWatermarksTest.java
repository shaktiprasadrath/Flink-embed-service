package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc02_eventtime.Uc02EventTimeWatermarks;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc02EventTimeWatermarksTest {

    @Test
    void tagsOnlyTheOutOfOrderClickAsLate() throws Exception {
        new Uc02EventTimeWatermarks().run();

        assertThat(Db.count("click_events")).isEqualTo(12);
        // watermark = max event time seen (10:06:00) - 10 s out-of-orderness - 1 ms
        assertThat(Db.rows("SELECT seq, user_id, page, event_time, watermark_before FROM click_events WHERE is_late"))
                .containsExactly("11|u2|/cart|2026-01-01 10:01:05|2026-01-01 10:05:49.999");
        assertThat(Db.rows("SELECT watermark_before FROM click_events WHERE seq IN (1, 2) ORDER BY seq"))
                .containsExactly("null", "2026-01-01 09:59:49.999");
    }
}
