package com.shakti.flinkdemo.usecases.uc13_cep;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.Times;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.common.Watermarks;
import com.shakti.flinkdemo.model.AuthEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Types;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * UC-13: Complex Event Processing (FlinkCEP). Detects a possible brute-force login:
 * exactly 3 consecutive FAIL events followed directly by a SUCCESS, all within 5 minutes (per user,
 * event time).
 * <ul>
 *   <li>alice: F F F S              -> MATCH</li>
 *   <li>bob:   F S                  -> no match (only 1 failure)</li>
 *   <li>carol: F F F F S            -> MATCH on the last 3 failures + success</li>
 *   <li>dave:  F F F ... S 17 min later -> TIMEOUT (partial match expired, via TimedOutPartialMatchHandler)</li>
 * </ul>
 */
public class Uc13ComplexEventProcessing implements UseCase {

    public static final String TABLE = "cep_matches";
    static final int FAILS = 3;

    public record Detection(String userId, long firstFail, Long success, int failCount, String status) {}

    static final OutputTag<Detection> TIMED_OUT = new OutputTag<>("timed-out") {};

    @Override
    public String id() {
        return "uc13";
    }

    @Override
    public String title() {
        return "CEP: pattern matching (3 failed logins then success within 5 min) + timeouts";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TABLE);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(TABLE, "user_id VARCHAR(10), first_fail TIMESTAMP, success_time TIMESTAMP, fail_count INT, "
                + "status VARCHAR(10)");

        StreamExecutionEnvironment env = Envs.local(2);

        DataStream<AuthEvent> events = Sources.events(env, DataFiles.AUTH_EVENTS, AuthEvent::parse, AuthEvent.class,
                Watermarks.<AuthEvent>perEvent(Duration.ofSeconds(5), (e, ts) -> e.eventTime()));

        Pattern<AuthEvent, ?> bruteForce = Pattern.<AuthEvent>begin("fails")
                .where(SimpleCondition.of(AuthEvent::isFail))
                .times(FAILS).consecutive()           // F F F with nothing in between
                .next("success")                      // strict contiguity: the very next event
                .where(SimpleCondition.of(AuthEvent::isSuccess))
                .within(Duration.ofMinutes(5));

        SingleOutputStreamOperator<Detection> matches = CEP
                .pattern(events.keyBy(AuthEvent::userId), bruteForce)
                .inEventTime()
                .process(new DetectionBuilder())
                .name("cep-brute-force");

        matches.union(matches.getSideOutput(TIMED_OUT))
                .sinkTo(JdbcSinks.sink("INSERT INTO " + TABLE + " VALUES (?, ?, ?, ?, ?)", (ps, d) -> {
                    ps.setString(1, d.userId());
                    ps.setTimestamp(2, Times.sql(d.firstFail()));
                    if (d.success() == null) {
                        ps.setNull(3, Types.TIMESTAMP);
                    } else {
                        ps.setTimestamp(3, Times.sql(d.success()));
                    }
                    ps.setInt(4, d.failCount());
                    ps.setString(5, d.status());
                }))
                .name("h2:" + TABLE);

        env.execute("uc13-cep");
    }

    static class DetectionBuilder extends PatternProcessFunction<AuthEvent, Detection>
            implements TimedOutPartialMatchHandler<AuthEvent> {

        @Override
        public void processMatch(Map<String, List<AuthEvent>> match, Context ctx, Collector<Detection> out) {
            List<AuthEvent> fails = match.get("fails");
            AuthEvent success = match.get("success").get(0);
            out.collect(new Detection(success.userId(), fails.get(0).eventTime(), success.eventTime(), fails.size(),
                    "MATCH"));
        }

        @Override
        public void processTimedOutMatch(Map<String, List<AuthEvent>> match, Context ctx) {
            List<AuthEvent> fails = match.get("fails");
            // only report partial matches that already had all failures and were waiting for the success
            if (fails != null && fails.size() == FAILS && !match.containsKey("success")) {
                ctx.output(TIMED_OUT, new Detection(fails.get(0).userId(), fails.get(0).eventTime(), null,
                        fails.size(), "TIMEOUT"));
            }
        }
    }
}
