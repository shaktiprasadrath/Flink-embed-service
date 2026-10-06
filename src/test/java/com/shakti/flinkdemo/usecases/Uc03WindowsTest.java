package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc03_windows.Uc03Windows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc03WindowsTest {

    private static final String COUNTS = "SELECT user_id, FORMATDATETIME(window_start, 'HH:mm:ss'), "
            + "FORMATDATETIME(window_end, 'HH:mm:ss'), clicks FROM ";

    @BeforeAll
    static void runOnce() throws Exception {
        new Uc03Windows().run();
    }

    @Test
    void tumblingWindowsOfTwoMinutes() {
        assertThat(Db.rows(COUNTS + "window_tumbling ORDER BY user_id, window_start")).containsExactly(
                "u1|10:00:00|10:02:00|4",
                "u1|10:06:00|10:08:00|1",
                "u2|10:00:00|10:02:00|2",
                "u2|10:02:00|10:04:00|1",
                "u3|10:00:00|10:02:00|1",
                "u3|10:02:00|10:04:00|1",
                "u3|10:06:00|10:08:00|1");
    }

    @Test
    void slidingWindowsOfTwoMinutesEveryMinute() {
        assertThat(Db.rows(COUNTS + "window_sliding ORDER BY user_id, window_start")).containsExactly(
                "u1|09:59:00|10:01:00|3",
                "u1|10:00:00|10:02:00|4",
                "u1|10:01:00|10:03:00|1",
                "u1|10:05:00|10:07:00|1",
                "u1|10:06:00|10:08:00|1",
                "u2|09:59:00|10:01:00|1",
                "u2|10:00:00|10:02:00|2",
                "u2|10:01:00|10:03:00|2",
                "u2|10:02:00|10:04:00|1",
                "u3|10:00:00|10:02:00|1",
                "u3|10:01:00|10:03:00|1",
                "u3|10:02:00|10:04:00|1",
                "u3|10:03:00|10:05:00|1",
                "u3|10:05:00|10:07:00|1",
                "u3|10:06:00|10:08:00|1");
    }

    @Test
    void sessionWindowsCloseAfterTwoMinutesOfInactivity() {
        assertThat(Db.rows(COUNTS + "window_session ORDER BY user_id, window_start")).containsExactly(
                "u1|10:00:00|10:03:30|4",
                "u1|10:06:00|10:08:00|1",
                "u2|10:00:30|10:04:40|3",
                "u3|10:01:15|10:05:00|2",
                "u3|10:06:30|10:08:30|1");
    }

    @Test
    void globalWindowWithCountTriggerAndEvictor() {
        // global windows never drop late data, so u2's late /cart click is included
        assertThat(Db.rows("SELECT * FROM window_global ORDER BY user_id, fire_seq")).containsExactly(
                "u1|1|/home|/products",
                "u1|2|/cart|/checkout",
                "u2|1|/home|/products",
                "u2|2|/home|/cart",
                "u3|1|/home|/products");
    }

    @Test
    void lateClickGoesToSideOutput() {
        assertThat(Db.rows("SELECT * FROM window_late_events")).containsExactly("u2|/cart|2026-01-01 10:01:05");
    }
}
