package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc19_metrics.Uc19MetricsAccumulators;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class Uc19MetricsAccumulatorsTest {

    @Test
    void reportsCustomMetricsAndAccumulator() throws Exception {
        new Uc19MetricsAccumulators().run();

        assertThat(Db.rows("SELECT * FROM metrics_demo ORDER BY metric_name")).containsExactly(
                "accumulator.hot-readings|5",                  // readings above 22.0
                "metric.lastTemperature|26.3",
                "metric.readings|12",
                "metric.readingsPerSecond|events=12",
                "metric.temperatureX10|count=12,min=187,max=263");
    }
}
