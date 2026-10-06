package com.shakti.flinkdemo.usecases.uc18_parallelism;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.model.SensorReading;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * UC-18: Parallelism, data exchange (partitioning), operator chaining and slot sharing.
 * <pre>
 *   source (p=1) -rebalance-> parse (p=4) -keyBy(sensor)-> max-per-sensor -> format (p=2, chained)
 *       -forward-> audit (p=2, chaining disabled) -rebalance-> sink (p=1, own slot sharing group)
 * </pre>
 * The job graph (one vertex per chain of operators) is saved to {@code job_vertices}. Each sensor is
 * always handled by the same keyed subtask: keyBy hashes the key to a key group.
 */
public class Uc18ParallelismChaining implements UseCase {

    public static final String TABLE = "parallelism_demo";
    public static final String VERTICES = "job_vertices";

    public record SensorMax(String sensorId, double maxTemperature, int readings, int keyedSubtask) {}

    @Override
    public String id() {
        return "uc18";
    }

    @Override
    public String title() {
        return "Parallelism, partitioning, operator chaining, slot sharing groups";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TABLE, VERTICES);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(TABLE, "sensor_id VARCHAR(10) PRIMARY KEY, max_temperature DECIMAL(5,1), readings INT, "
                + "keyed_subtask INT");
        Db.recreate(VERTICES, "vertex_name VARCHAR(400), parallelism INT");

        Configuration conf = new Configuration();
        // default group needs 4 slots (max parallelism); the sink's own group needs 1 more
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 8);
        StreamExecutionEnvironment env = Envs.local(4, conf);

        Sources.lines(env, DataFiles.SENSORS)
                .rebalance()                                      // round-robin to the 4 parse subtasks
                .map(SensorReading::parse).name("parse").setParallelism(4)
                .keyBy(SensorReading::sensorId)                   // hash partitioning by sensor
                .process(new MaxPerSensor()).name("max-per-sensor").setParallelism(2)
                .map(m -> m).returns(SensorMax.class).name("format").setParallelism(2) // chained to max-per-sensor
                .map(m -> m).returns(SensorMax.class).name("audit").setParallelism(2)
                .disableChaining()                                // forces its own task
                .sinkTo(JdbcSinks.sink("MERGE INTO " + TABLE + " KEY (sensor_id) VALUES (?, ?, ?, ?)", (ps, m) -> {
                    ps.setString(1, m.sensorId());
                    ps.setBigDecimal(2, BigDecimal.valueOf(m.maxTemperature()));
                    ps.setInt(3, m.readings());
                    ps.setInt(4, m.keyedSubtask());
                }))
                .name("h2:" + TABLE)
                .setParallelism(1)
                .slotSharingGroup("sink-group");

        // getStreamGraph(false) keeps the pipeline so it can still be executed below
        JobGraph jobGraph = env.getStreamGraph(false).getJobGraph();
        try (Connection c = Db.connect();
             PreparedStatement insert = c.prepareStatement("INSERT INTO " + VERTICES + " VALUES (?, ?)")) {
            for (JobVertex vertex : jobGraph.getVertices()) {
                insert.setString(1, vertex.getName());
                insert.setInt(2, vertex.getParallelism());
                insert.executeUpdate();
            }
        }
        System.out.println("  execution plan (paste into https://flink.apache.org/visualizer/):");
        System.out.println("  " + env.getExecutionPlan().replace("\n", "\n  "));

        env.execute("uc18-parallelism-chaining");
    }

    static class MaxPerSensor extends KeyedProcessFunction<String, SensorReading, SensorMax> {
        private transient ValueState<SensorMax> max;

        @Override
        public void open(OpenContext openContext) {
            max = getRuntimeContext().getState(new ValueStateDescriptor<>("max", SensorMax.class));
        }

        @Override
        public void processElement(SensorReading r, Context ctx, Collector<SensorMax> out) throws Exception {
            int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            SensorMax current = max.value();
            SensorMax next = current == null
                    ? new SensorMax(r.sensorId(), r.temperature(), 1, subtask)
                    : new SensorMax(r.sensorId(), Math.max(current.maxTemperature(), r.temperature()),
                    current.readings() + 1, subtask);
            max.update(next);
            out.collect(next);
        }
    }
}
