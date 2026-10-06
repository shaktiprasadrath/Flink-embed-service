package com.shakti.flinkdemo.usecases.uc04_keyedstate;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.model.Click;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * UC-04: Keyed state. Per user it keeps
 * <ul>
 *   <li>ValueState  - total clicks</li>
 *   <li>ListState   - the last 3 pages visited</li>
 *   <li>MapState    - visits per page (with a state TTL, so idle entries expire after 1 hour)</li>
 * </ul>
 * and writes a profile row per user (upserted on every click).
 */
public class Uc04KeyedState implements UseCase {

    public static final String TABLE = "user_state";

    public record UserProfile(String userId, long totalClicks, int distinctPages, String lastPages, String topPage) {}

    @Override
    public String id() {
        return "uc04";
    }

    @Override
    public String title() {
        return "Keyed state: ValueState, ListState, MapState, state TTL";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TABLE);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(TABLE, "user_id VARCHAR(10) PRIMARY KEY, total_clicks INT, distinct_pages INT, "
                + "last_pages VARCHAR(100), top_page VARCHAR(40)");

        StreamExecutionEnvironment env = Envs.local(2);

        Sources.lines(env, DataFiles.CLICKS)
                .map(Click::parse).name("parse-clicks").setParallelism(1)
                .keyBy(Click::userId)
                .process(new ProfileBuilder())
                .name("user-profile-state")
                .sinkTo(JdbcSinks.sink("MERGE INTO " + TABLE + " KEY (user_id) VALUES (?, ?, ?, ?, ?)", (ps, p) -> {
                    ps.setString(1, p.userId());
                    ps.setLong(2, p.totalClicks());
                    ps.setInt(3, p.distinctPages());
                    ps.setString(4, p.lastPages());
                    ps.setString(5, p.topPage());
                }))
                .name("h2:" + TABLE);

        env.execute("uc04-keyed-state");
    }

    static class ProfileBuilder extends KeyedProcessFunction<String, Click, UserProfile> {
        private static final int RECENT = 3;

        private transient ValueState<Long> totalClicks;
        private transient ListState<String> recentPages;
        private transient MapState<String, Long> pageVisits;

        @Override
        public void open(OpenContext openContext) {
            totalClicks = getRuntimeContext().getState(new ValueStateDescriptor<>("total-clicks", Long.class));
            recentPages = getRuntimeContext().getListState(new ListStateDescriptor<>("recent-pages", String.class));

            MapStateDescriptor<String, Long> visits = new MapStateDescriptor<>("page-visits", String.class, Long.class);
            visits.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofHours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build());
            pageVisits = getRuntimeContext().getMapState(visits);
        }

        @Override
        public void processElement(Click click, Context ctx, Collector<UserProfile> out) throws Exception {
            long total = totalClicks.value() == null ? 1 : totalClicks.value() + 1;
            totalClicks.update(total);

            List<String> recent = new ArrayList<>();
            recentPages.get().forEach(recent::add);
            recent.add(click.page());
            if (recent.size() > RECENT) {
                recent = recent.subList(recent.size() - RECENT, recent.size());
            }
            recentPages.update(recent);

            Long visits = pageVisits.get(click.page());
            pageVisits.put(click.page(), visits == null ? 1 : visits + 1);

            int distinct = 0;
            String topPage = null;
            long topCount = 0;
            for (Map.Entry<String, Long> e : pageVisits.entries()) {
                distinct++;
                // highest count wins; ties go to the alphabetically first page (deterministic)
                if (e.getValue() > topCount || (e.getValue() == topCount && e.getKey().compareTo(topPage) < 0)) {
                    topPage = e.getKey();
                    topCount = e.getValue();
                }
            }
            out.collect(new UserProfile(ctx.getCurrentKey(), total, distinct, String.join(",", recent), topPage));
        }
    }
}
