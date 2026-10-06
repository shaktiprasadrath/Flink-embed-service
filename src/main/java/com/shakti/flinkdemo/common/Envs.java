package com.shakti.flinkdemo.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Creates execution environments. When run from the app (not from a Flink cluster),
 * {@code getExecutionEnvironment} starts an embedded MiniCluster inside this JVM.
 */
public final class Envs {

    private Envs() {}

    public static StreamExecutionEnvironment local(int parallelism) {
        return local(parallelism, new Configuration());
    }

    public static StreamExecutionEnvironment local(int parallelism, Configuration conf) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        return env;
    }

    /** Table environment in streaming mode on top of a local DataStream environment. */
    public static StreamTableEnvironment table(StreamExecutionEnvironment env) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());
        // Interpret TIMESTAMP_LTZ / PROCTIME in UTC so output is the same on every machine.
        tEnv.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
        return tEnv;
    }
}
