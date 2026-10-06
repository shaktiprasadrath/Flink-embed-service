package com.shakti.flinkdemo.usecases.uc19_metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Minimal custom metric reporter. Flink registers every metric with it; when a metric is removed
 * (the task finished) its last value is kept, so the demo can store final values in H2.
 * Real deployments use reporters such as Prometheus, JMX or Slf4j in the same way.
 */
public class CollectingMetricReporter implements MetricReporter {

    /** Only metrics in this user group are collected. */
    static final String GROUP = "flinkdemo";

    private static final Map<String, Metric> LIVE = new TreeMap<>();
    private static final Map<String, String> FINAL_VALUES = new TreeMap<>();

    public static synchronized void reset() {
        LIVE.clear();
        FINAL_VALUES.clear();
    }

    /**
     * Last value of every collected metric. Tasks may still be unregistering their metrics when
     * {@code execute()} returns, so metrics not yet removed are read directly (they no longer change).
     */
    public static synchronized Map<String, String> finalValues() {
        Map<String, String> values = new TreeMap<>(FINAL_VALUES);
        LIVE.forEach((id, metric) -> values.put(shortName(id), valueOf(metric)));
        return values;
    }

    private static String shortName(String id) {
        return id.substring(id.indexOf(GROUP + ".") + GROUP.length() + 1);
    }

    @Override
    public void open(MetricConfig config) {}

    @Override
    public void close() {}

    @Override
    public synchronized void notifyOfAddedMetric(Metric metric, String name, MetricGroup group) {
        String id = group.getMetricIdentifier(name);
        if (id.contains("." + GROUP + ".")) {
            LIVE.put(id, metric);
        }
    }

    @Override
    public synchronized void notifyOfRemovedMetric(Metric metric, String name, MetricGroup group) {
        String id = group.getMetricIdentifier(name);
        if (LIVE.remove(id) != null) {
            FINAL_VALUES.put(shortName(id), valueOf(metric));
        }
    }

    static String valueOf(Metric metric) {
        if (metric instanceof Counter c) {
            return Long.toString(c.getCount());
        }
        if (metric instanceof Gauge<?> g) {
            return String.valueOf(g.getValue());
        }
        if (metric instanceof Histogram h) {
            return "count=" + h.getCount() + ",min=" + h.getStatistics().getMin() + ",max=" + h.getStatistics().getMax();
        }
        if (metric instanceof Meter m) {
            return "events=" + m.getCount();
        }
        return metric.toString();
    }

    /**
     * Factory referenced by the 'metrics.reporter.collect.factory.class' option. Flink finds factories
     * with the ServiceLoader: see META-INF/services/org.apache.flink.metrics.reporter.MetricReporterFactory.
     */
    public static class Factory implements MetricReporterFactory {
        @Override
        public MetricReporter createMetricReporter(Properties properties) {
            return new CollectingMetricReporter();
        }
    }
}
