package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc17_batch.Uc17BatchExecutionMode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc17BatchExecutionModeTest {

    @Test
    void batchEmitsOnlyFinalResultsStreamingEmitsUpdates() throws Exception {
        new Uc17BatchExecutionMode().run();

        assertThat(Db.rows("SELECT execution_mode, COUNT(*) FROM batch_vs_stream GROUP BY execution_mode "
                + "ORDER BY execution_mode")).containsExactly("BATCH|4", "STREAMING|10");
        assertThat(Db.rows("SELECT product, total_quantity FROM batch_vs_stream WHERE execution_mode = 'BATCH' "
                + "ORDER BY product")).containsExactly("keyboard|4", "laptop|3", "monitor|3", "mouse|9");
        assertThat(Db.rows("SELECT product, MAX(total_quantity) FROM batch_vs_stream WHERE execution_mode = "
                + "'STREAMING' GROUP BY product ORDER BY product"))
                .containsExactly("keyboard|4", "laptop|3", "monitor|3", "mouse|9");
    }
}
