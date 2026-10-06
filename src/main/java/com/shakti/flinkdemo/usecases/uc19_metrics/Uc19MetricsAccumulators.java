package com.shakti.flinkdemo.usecases.uc19_metrics;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.model.SensorReading;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

/**
 * UC-19: Metrics and accumulators.
 * <ul>
 *   <li>Custom metrics in an operator: Counter, Gauge, Histogram, Meter (group "flinkdemo")</li>
 *   <li>A custom metric reporter ({@link CollectingMetricReporter}) configured via
 *       {@code metrics.reporter.<name>.factory.class}</li>
 *   <li>An accumulator, read from the JobExecutionResult when the job finishes</li>
 * </ul>
 */
public class Uc19MetricsAccumulators implements UseCase {

    public static final String TABLE = "metrics_demo";
    static final String ACCUMULATOR = "hot-readings";
    static final double HOT = 22.0;

    @Override
    public String id() {
        return "uc19";
    }

    @Override
    public String title() {
        return "Metrics (counter, gauge, histogram, meter), custom reporter, accumulators";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TABLE);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(TABLE, "metric_name VARCHAR(100) PRIMARY KEY, metric_value VARCHAR(200)");
        CollectingMetricReporter.reset();

        Configuration conf = new Configuration();
        conf.setString("metrics.reporter.collect.factory.class", CollectingMetricReporter.Factory.class.getName());
        StreamExecutionEnvironment env = Envs.local(1, conf);

        Sources.lines(env, DataFiles.SENSORS)
                .map(SensorReading::parse).name("parse")
                .map(new Instrumented()).name("instrumented")
                .sinkTo(new DiscardingSink<>()).name("discard");

        JobExecutionResult result = env.execute("uc19-metrics");
        Integer hot = result.getAccumulatorResult(ACCUMULATOR);

        try (Connection c = Db.connect();
             PreparedStatement insert = c.prepareStatement("INSERT INTO " + TABLE + " VALUES (?, ?)")) {
            insert.setString(1, "accumulator." + ACCUMULATOR);
            insert.setString(2, String.valueOf(hot));
            insert.executeUpdate();
            for (Map.Entry<String, String> metric : CollectingMetricReporter.finalValues().entrySet()) {
                insert.setString(1, "metric." + metric.getKey());
                insert.setString(2, metric.getValue());
                insert.executeUpdate();
            }
        }
    }

    static class Instrumented extends RichMapFunction<SensorReading, SensorReading> {
        private transient Counter readings;
        private transient Meter readingsRate;
        private transient Histogram temperatureX10;
        private transient volatile double lastTemperature;
        private final IntCounter hotReadings = new IntCounter();

        @Override
        public void open(OpenContext openContext) {
            MetricGroup group = getRuntimeContext().getMetricGroup().addGroup(CollectingMetricReporter.GROUP);
            readings = group.counter("readings");
            readingsRate = group.meter("readingsPerSecond", new MeterView(readings));
            temperatureX10 = group.histogram("temperatureX10", new DescriptiveStatisticsHistogram(1000));
            group.gauge("lastTemperature", () -> lastTemperature);
            getRuntimeContext().addAccumulator(ACCUMULATOR, hotReadings);
        }

        @Override
        public SensorReading map(SensorReading r) {
            readings.inc();
            temperatureX10.update(Math.round(r.temperature() * 10));
            lastTemperature = r.temperature();
            if (r.temperature() > HOT) {
                hotReadings.add(1);
            }
            return r;
        }
    }
}
