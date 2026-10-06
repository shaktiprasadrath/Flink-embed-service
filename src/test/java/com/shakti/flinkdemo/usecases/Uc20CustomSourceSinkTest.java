package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc20_customio.Uc20CustomSourceSink;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc20CustomSourceSinkTest {

    @Test
    void customSourceReadsEveryLineAndCustomSinkWritesIt() throws Exception {
        new Uc20CustomSourceSink().run();

        assertThat(Db.rows("SELECT file_name, COUNT(*), MIN(line_no), MAX(line_no) FROM custom_io_demo "
                + "GROUP BY file_name ORDER BY file_name"))
                .containsExactly("sensors.csv|13|1|13", "users.csv|5|1|5");
        assertThat(Db.rows("SELECT line FROM custom_io_demo WHERE file_name = 'users.csv' AND line_no = 2"))
                .containsExactly("U1,Alice Smith,alice@example.com,US,music;sports;tech");
    }
}
