package com.shakti.flinkdemo.usecases.uc02_eventtime;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.Times;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.common.Watermarks;
import com.shakti.flinkdemo.model.Click;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Types;
import java.time.Duration;
import java.util.List;

/**
 * UC-02: Event time and watermarks.
 *
 * <p>Timestamps come from the event ("ts" field), not from the machine clock. A bounded
 * out-of-orderness watermark (10 s) tells Flink "no events older than max_seen - 10 s are expected".
 * Each record is written with the watermark that was current when it arrived; a record whose
 * timestamp is not newer than that watermark is late (the u2 /cart click at 10:01:05).
 * {@code withIdleness} keeps watermarks moving if a source split stops sending data.
 */
public class Uc02EventTimeWatermarks implements UseCase {

    public static final String TABLE = "click_events";

    public record ClickEvent(int seq, String userId, String page, long eventTime, long watermarkBefore, boolean late) {}

    @Override
    public String id() {
        return "uc02";
    }

    @Override
    public String title() {
        return "Event time, watermarks, late events, idleness";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TABLE);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(TABLE, "seq INT PRIMARY KEY, user_id VARCHAR(10), page VARCHAR(40), event_time TIMESTAMP(3), "
                + "watermark_before TIMESTAMP(3), is_late BOOLEAN");

        StreamExecutionEnvironment env = Envs.local(1);

        WatermarkStrategy<Click> watermarks = Watermarks
                .<Click>perEvent(Duration.ofSeconds(10), (click, previousTs) -> click.ts())
                .withIdleness(Duration.ofMinutes(1));

        Sources.events(env, DataFiles.CLICKS, Click::parse, Click.class, watermarks)
                .process(new ProcessFunction<Click, ClickEvent>() {
                    private int seq;

                    @Override
                    public void processElement(Click click, Context ctx, Collector<ClickEvent> out) {
                        long eventTime = ctx.timestamp();
                        long watermark = ctx.timerService().currentWatermark();
                        out.collect(new ClickEvent(++seq, click.userId(), click.page(), eventTime, watermark,
                                eventTime <= watermark));
                    }
                })
                .name("tag-late-events")
                .sinkTo(JdbcSinks.sink("INSERT INTO " + TABLE + " VALUES (?, ?, ?, ?, ?, ?)", (ps, e) -> {
                    ps.setInt(1, e.seq());
                    ps.setString(2, e.userId());
                    ps.setString(3, e.page());
                    ps.setTimestamp(4, Times.sql(e.eventTime()));
                    if (e.watermarkBefore() == Long.MIN_VALUE) {
                        ps.setNull(5, Types.TIMESTAMP); // no watermark yet before the first event
                    } else {
                        ps.setTimestamp(5, Times.sql(e.watermarkBefore()));
                    }
                    ps.setBoolean(6, e.late());
                }))
                .name("h2:" + TABLE);

        env.execute("uc02-event-time-watermarks");
    }
}
