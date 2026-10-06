package com.shakti.flinkdemo.usecases.uc16_savepoints;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.model.Click;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * UC-16: RocksDB state backend, savepoints, stop and resume.
 *
 * <ol>
 *   <li>Phase 1: an unbounded job (FileSource monitoring a folder) counts clicks per user in RocksDB
 *       state. Only clicks-part1.jsonl is in the folder.</li>
 *   <li>The job is stopped with a savepoint (a consistent snapshot of all state, written by
 *       {@code stopWithSavepoint}).</li>
 *   <li>clicks-part2.jsonl is added to the folder.</li>
 *   <li>Phase 2: a new job is started from the savepoint. Counts continue from the saved state, and
 *       the source remembers part 1 was already read, so it only reads part 2.</li>
 * </ol>
 * Operators have stable {@code uid}s so state can be mapped back after a restart or code change.
 */
public class Uc16SavepointsStateBackend implements UseCase {

    private static final Logger LOG = LoggerFactory.getLogger(Uc16SavepointsStateBackend.class);

    public static final String TABLE = "savepoint_demo";
    static final Duration WAIT = Duration.ofSeconds(90);

    public record UserCount(String userId, long clicks, int phase) {}

    private String lastSavepoint;

    @Override
    public String id() {
        return "uc16";
    }

    @Override
    public String title() {
        return "RocksDB state backend + stop-with-savepoint + resume from savepoint";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TABLE);
    }

    public String lastSavepoint() {
        return lastSavepoint;
    }

    @Override
    public void run() throws Exception {
        Db.recreate(TABLE, "user_id VARCHAR(10) PRIMARY KEY, clicks BIGINT, written_by_phase INT");

        java.nio.file.Path work = Files.createTempDirectory("flinkdemo-uc16-");
        java.nio.file.Path input = Files.createDirectories(work.resolve("input"));
        String checkpoints = work.resolve("checkpoints").toUri().toString();
        String savepoints = work.resolve("savepoints").toUri().toString();

        // Phase 1
        addFile(input, "clicks-part1.jsonl");
        JobClient phase1 = buildJob(input, checkpoints, savepoints, 1, null).executeAsync("uc16-phase-1");
        waitFor(() -> totalClicks() == 6, "phase 1 to process part 1", phase1);
        lastSavepoint = phase1.stopWithSavepoint(false, savepoints, SavepointFormatType.CANONICAL)
                .get(WAIT.toSeconds(), TimeUnit.SECONDS);
        LOG.info("Phase 1 stopped with savepoint {}", lastSavepoint);

        // Phase 2
        addFile(input, "clicks-part2.jsonl");
        JobClient phase2 = buildJob(input, checkpoints, savepoints, 2, lastSavepoint).executeAsync("uc16-phase-2");
        try {
            waitFor(() -> totalClicks() == 12, "phase 2 to process part 2", phase2);
        } finally {
            phase2.cancel().get(WAIT.toSeconds(), TimeUnit.SECONDS);
        }
    }

    private StreamExecutionEnvironment buildJob(java.nio.file.Path input, String checkpoints, String savepoints,
                                                int phase, String restoreFrom) {
        Configuration conf = new Configuration();
        conf.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpoints);
        conf.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepoints);
        if (restoreFrom != null) {
            conf.set(StateRecoveryOptions.SAVEPOINT_PATH, restoreFrom);
        }
        StreamExecutionEnvironment env = Envs.local(2, conf);
        env.enableCheckpointing(1000);

        FileSource<String> folder = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(input.toUri()))
                .monitorContinuously(Duration.ofMillis(200)) // unbounded: picks up new files
                .build();

        env.fromSource(folder, WatermarkStrategy.noWatermarks(), "watch-clicks-folder")
                .uid("clicks-folder-source")
                .setParallelism(1)
                .map(Click::parse).uid("parse-clicks").name("parse-clicks")
                .keyBy(Click::userId)
                .process(new CountClicks(phase)).uid("click-counter").name("click-counter")
                .sinkTo(JdbcSinks.sink("MERGE INTO " + TABLE + " KEY (user_id) VALUES (?, ?, ?)", (ps, c) -> {
                    ps.setString(1, c.userId());
                    ps.setLong(2, c.clicks());
                    ps.setInt(3, c.phase());
                }, 1, 0))
                .uid("h2-sink").name("h2:" + TABLE);
        return env;
    }

    /** Copy under a hidden name, then rename, so the source never sees a half-written file. */
    private static void addFile(java.nio.file.Path folder, String name) throws Exception {
        java.nio.file.Path hidden = folder.resolve("." + name);
        Files.copy(DataFiles.path("savepoint/" + name), hidden, StandardCopyOption.REPLACE_EXISTING);
        Files.move(hidden, folder.resolve(name), StandardCopyOption.ATOMIC_MOVE);
    }

    private static long totalClicks() {
        return Long.parseLong(Db.rows("SELECT COALESCE(SUM(clicks), 0) FROM " + TABLE).get(0));
    }

    private static void waitFor(BooleanSupplier condition, String what, JobClient job) throws Exception {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline || job.getJobStatus().get().isGloballyTerminalState()) {
                job.cancel();
                throw new IllegalStateException("Timed out waiting for " + what + "; job status "
                        + job.getJobStatus().get());
            }
            Thread.sleep(100);
        }
    }

    static class CountClicks extends KeyedProcessFunction<String, Click, UserCount> {
        private final int phase;
        private transient ValueState<Long> count;

        CountClicks(int phase) {
            this.phase = phase;
        }

        @Override
        public void open(OpenContext openContext) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("clicks", Long.class));
        }

        @Override
        public void processElement(Click click, Context ctx, Collector<UserCount> out) throws Exception {
            long next = count.value() == null ? 1 : count.value() + 1;
            count.update(next);
            out.collect(new UserCount(click.userId(), next, phase));
        }
    }
}
