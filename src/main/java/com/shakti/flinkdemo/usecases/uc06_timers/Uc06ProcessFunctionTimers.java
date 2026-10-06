package com.shakti.flinkdemo.usecases.uc06_timers;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.Times;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.common.Watermarks;
import com.shakti.flinkdemo.model.Click;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * UC-06: KeyedProcessFunction with event-time timers. Raises an inactivity alert when a user has
 * no click for 2 minutes (event time).
 *
 * <ul>
 *   <li>Each click (re)registers a timer at last_seen + 2 min and deletes the previous one.</li>
 *   <li>{@code onTimer} fires once the watermark passes that time: alert detected by TIMER.</li>
 *   <li>A watermark can jump past the timer together with the next click (u1: 10:01:30 then
 *       10:06:00). The gap is then detected when the click is processed: alert by ON_EVENT.</li>
 *   <li>Late clicks (timestamp at or below the watermark) are ignored.</li>
 *   <li>When the bounded input ends, Flink sends a final watermark, so all pending timers fire.</li>
 * </ul>
 * Processing-time timers work the same way via {@code registerProcessingTimeTimer}.
 */
public class Uc06ProcessFunctionTimers implements UseCase {

    public static final String TABLE = "inactivity_alerts";
    static final long GAP = Duration.ofMinutes(2).toMillis();

    public record Alert(String userId, long lastSeen, long alertTime, String detectedBy) {}

    @Override
    public String id() {
        return "uc06";
    }

    @Override
    public String title() {
        return "ProcessFunction + event-time timers: inactivity alerts";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TABLE);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(TABLE, "user_id VARCHAR(10), last_seen TIMESTAMP, alert_time TIMESTAMP, detected_by VARCHAR(10)");

        StreamExecutionEnvironment env = Envs.local(2);

        Sources.events(env, DataFiles.CLICKS, Click::parse, Click.class,
                        Watermarks.<Click>perEvent(Duration.ofSeconds(10), (c, ts) -> c.ts()))
                .keyBy(Click::userId)
                .process(new InactivityDetector())
                .name("inactivity-timers")
                .sinkTo(JdbcSinks.sink("INSERT INTO " + TABLE + " VALUES (?, ?, ?, ?)", (ps, a) -> {
                    ps.setString(1, a.userId());
                    ps.setTimestamp(2, Times.sql(a.lastSeen()));
                    ps.setTimestamp(3, Times.sql(a.alertTime()));
                    ps.setString(4, a.detectedBy());
                }))
                .name("h2:" + TABLE);

        env.execute("uc06-process-function-timers");
    }

    static class InactivityDetector extends KeyedProcessFunction<String, Click, Alert> {
        private transient ValueState<Long> lastSeen;

        @Override
        public void open(OpenContext openContext) {
            lastSeen = getRuntimeContext().getState(new ValueStateDescriptor<>("last-seen", Long.class));
        }

        @Override
        public void processElement(Click click, Context ctx, Collector<Alert> out) throws Exception {
            long ts = ctx.timestamp();
            if (ts <= ctx.timerService().currentWatermark()) {
                return; // late event
            }
            Long previous = lastSeen.value();
            if (previous != null) {
                if (ts < previous) {
                    return; // out of order but not late: activity already covered
                }
                ctx.timerService().deleteEventTimeTimer(previous + GAP);
                if (ts - previous >= GAP) {
                    out.collect(new Alert(ctx.getCurrentKey(), previous, previous + GAP, "ON_EVENT"));
                }
            }
            lastSeen.update(ts);
            ctx.timerService().registerEventTimeTimer(ts + GAP);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
            out.collect(new Alert(ctx.getCurrentKey(), lastSeen.value(), timestamp, "TIMER"));
            lastSeen.clear(); // next click starts a new activity period
        }
    }
}
