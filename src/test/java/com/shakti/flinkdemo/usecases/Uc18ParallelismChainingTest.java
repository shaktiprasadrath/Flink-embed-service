package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc18_parallelism.Uc18ParallelismChaining;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc18ParallelismChainingTest {

    @BeforeAll
    static void runOnce() throws Exception {
        new Uc18ParallelismChaining().run();
    }

    @Test
    void computesMaxPerSensorAcrossParallelSubtasks() {
        assertThat(Db.rows("SELECT sensor_id, max_temperature, readings FROM parallelism_demo ORDER BY sensor_id"))
                .containsExactly("S1|23.2|3", "S2|22.8|3", "S3|19.5|3", "S4|26.3|3");
        assertThat(Db.rows("SELECT DISTINCT keyed_subtask FROM parallelism_demo"))
                .allSatisfy(subtask -> assertThat(subtask).isIn("0", "1"));
    }

    @Test
    void jobGraphShowsChainingAndParallelism() {
        assertThat(Db.rows("SELECT vertex_name, parallelism FROM job_vertices"))
                .anySatisfy(v -> assertThat(v).contains("parse").endsWith("|4"))
                .anySatisfy(v -> assertThat(v).contains("max-per-sensor").contains("format").endsWith("|2"))
                .anySatisfy(v -> assertThat(v).startsWith("audit|2"))
                .anySatisfy(v -> assertThat(v).contains("h2:parallelism_demo").endsWith("|1"));
    }
}
