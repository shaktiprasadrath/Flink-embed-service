package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc06_timers.Uc06ProcessFunctionTimers;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc06ProcessFunctionTimersTest {

    @Test
    void raisesInactivityAlertsFromTimers() throws Exception {
        new Uc06ProcessFunctionTimers().run();

        assertThat(Db.rows("SELECT user_id, FORMATDATETIME(last_seen, 'HH:mm:ss'), "
                + "FORMATDATETIME(alert_time, 'HH:mm:ss'), detected_by FROM inactivity_alerts "
                + "ORDER BY user_id, last_seen"))
                .containsExactly(
                        "u1|10:01:30|10:03:30|ON_EVENT",
                        "u1|10:06:00|10:08:00|TIMER",
                        "u2|10:02:40|10:04:40|TIMER",
                        "u3|10:03:00|10:05:00|TIMER",
                        "u3|10:06:30|10:08:30|TIMER");
    }
}
