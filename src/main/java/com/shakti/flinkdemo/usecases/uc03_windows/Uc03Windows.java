package com.shakti.flinkdemo.usecases.uc03_windows;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.Times;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.common.Watermarks;
import com.shakti.flinkdemo.model.Click;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * UC-03: Windows. Counts clicks per user with every window type:
 * tumbling (2 min), sliding (2 min every 1 min), session (2 min gap), and a global window with a
 * custom trigger (fire every 2 clicks) and evictor (keep last 2 clicks).
 * Late events of the tumbling window go to a side output instead of being silently dropped.
 */
public class Uc03Windows implements UseCase {

    public static final String TUMBLING = "window_tumbling";
    public static final String SLIDING = "window_sliding";
    public static final String SESSION = "window_session";
    public static final String GLOBAL = "window_global";
    public static final String LATE = "window_late_events";

    public record WindowCount(String userId, long windowStart, long windowEnd, long clicks) {}

    public record GlobalFire(String userId, int fireSeq, String pages) {}

    @Override
    public String id() {
        return "uc03";
    }

    @Override
    public String title() {
        return "Windows: tumbling, sliding, session, global + trigger/evictor, late side output";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TUMBLING, SLIDING, SESSION, GLOBAL, LATE);
    }

    @Override
    public void run() throws Exception {
        String countColumns = "user_id VARCHAR(10), window_start TIMESTAMP, window_end TIMESTAMP, clicks INT";
        Db.recreate(TUMBLING, countColumns);
        Db.recreate(SLIDING, countColumns);
        Db.recreate(SESSION, countColumns);
        Db.recreate(GLOBAL, "user_id VARCHAR(10), fire_seq INT, pages VARCHAR(100)");
        Db.recreate(LATE, "user_id VARCHAR(10), page VARCHAR(40), event_time TIMESTAMP");

        StreamExecutionEnvironment env = Envs.local(2);

        KeyedStream<Click, String> clicksByUser = Sources
                .events(env, DataFiles.CLICKS, Click::parse, Click.class,
                        Watermarks.<Click>perEvent(Duration.ofSeconds(10), (c, ts) -> c.ts()))
                .keyBy(Click::userId);

        OutputTag<Click> lateTag = new OutputTag<>("late-clicks") {};

        SingleOutputStreamOperator<WindowCount> tumbling = clicksByUser
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(2)))
                .sideOutputLateData(lateTag)
                .aggregate(new CountClicks(), new WithWindow())
                .name("tumbling-2m");
        writeCounts(tumbling, TUMBLING);

        writeCounts(clicksByUser
                .window(SlidingEventTimeWindows.of(Duration.ofMinutes(2), Duration.ofMinutes(1)))
                .aggregate(new CountClicks(), new WithWindow())
                .name("sliding-2m-every-1m"), SLIDING);

        writeCounts(clicksByUser
                .window(EventTimeSessionWindows.withGap(Duration.ofMinutes(2)))
                .aggregate(new CountClicks(), new WithWindow())
                .name("session-gap-2m"), SESSION);

        clicksByUser
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(2))   // fire every 2nd click of a user
                .evictor(CountEvictor.of(2))   // the window function only sees the last 2 clicks
                .process(new LastPages())
                .name("global-count-trigger")
                .sinkTo(JdbcSinks.sink("INSERT INTO " + GLOBAL + " VALUES (?, ?, ?)", (ps, f) -> {
                    ps.setString(1, f.userId());
                    ps.setInt(2, f.fireSeq());
                    ps.setString(3, f.pages());
                }))
                .name("h2:" + GLOBAL);

        tumbling.getSideOutput(lateTag)
                .sinkTo(JdbcSinks.sink("INSERT INTO " + LATE + " VALUES (?, ?, ?)", (ps, c) -> {
                    ps.setString(1, c.userId());
                    ps.setString(2, c.page());
                    ps.setTimestamp(3, Times.sql(c.ts()));
                }))
                .name("h2:" + LATE);

        env.execute("uc03-windows");
    }

    private static void writeCounts(DataStream<WindowCount> counts, String table) {
        counts.sinkTo(JdbcSinks.sink("INSERT INTO " + table + " VALUES (?, ?, ?, ?)", (ps, w) -> {
            ps.setString(1, w.userId());
            ps.setTimestamp(2, Times.sql(w.windowStart()));
            ps.setTimestamp(3, Times.sql(w.windowEnd()));
            ps.setLong(4, w.clicks());
        })).name("h2:" + table);
    }

    /** Incremental pre-aggregation: only a counter is kept per window, not every click. */
    static class CountClicks implements AggregateFunction<Click, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Click value, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b; // needed for session windows, which merge
        }
    }

    /** Adds window metadata (start/end) to the pre-aggregated count. */
    static class WithWindow extends ProcessWindowFunction<Long, WindowCount, String, TimeWindow> {
        @Override
        public void process(String user, Context ctx, Iterable<Long> counts, Collector<WindowCount> out) {
            out.collect(new WindowCount(user, ctx.window().getStart(), ctx.window().getEnd(), counts.iterator().next()));
        }
    }

    static class LastPages extends ProcessWindowFunction<Click, GlobalFire, String, GlobalWindow> {
        @Override
        public void process(String user, Context ctx, Iterable<Click> clicks, Collector<GlobalFire> out) throws Exception {
            // per-key state that lives across firings of the same (global) window
            ValueState<Integer> fires = ctx.globalState().getState(new ValueStateDescriptor<>("fires", Integer.class));
            int seq = fires.value() == null ? 1 : fires.value() + 1;
            fires.update(seq);
            List<String> pages = new ArrayList<>();
            clicks.forEach(c -> pages.add(c.page()));
            out.collect(new GlobalFire(user, seq, String.join("|", pages)));
        }
    }
}
