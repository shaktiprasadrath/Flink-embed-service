package com.shakti.flinkdemo.usecases.uc12_udf;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.H2ChangelogSink;
import com.shakti.flinkdemo.common.TableDdl;
import com.shakti.flinkdemo.common.UseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * UC-12: User-defined functions in SQL.
 * <ul>
 *   <li>Scalar function  MASK_EMAIL(email): one value in, one value out</li>
 *   <li>Table function   SPLIT_INTERESTS(list): one value in, many rows out (LATERAL TABLE)</li>
 *   <li>Aggregate function TOP_INTEREST(interest): many rows in, one value out per group</li>
 * </ul>
 */
public class Uc12UserDefinedFunctions implements UseCase {

    public static final String USERS = "udf_users";
    public static final String INTERESTS = "udf_interests";
    public static final String COUNTRIES = "udf_country";

    @Override
    public String id() {
        return "uc12";
    }

    @Override
    public String title() {
        return "UDFs: scalar, table (LATERAL TABLE) and aggregate functions";
    }

    @Override
    public List<String> outputTables() {
        return List.of(USERS, INTERESTS, COUNTRIES);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(USERS, "user_id VARCHAR(10) PRIMARY KEY, name_upper VARCHAR(40), masked_email VARCHAR(60), "
                + "country VARCHAR(2)");
        Db.recreate(INTERESTS, "user_id VARCHAR(10), interest VARCHAR(20), PRIMARY KEY (user_id, interest)");
        Db.recreate(COUNTRIES, "country VARCHAR(2) PRIMARY KEY, users BIGINT, top_interest VARCHAR(20)");

        StreamExecutionEnvironment env = Envs.local(1);
        StreamTableEnvironment tEnv = Envs.table(env);
        TableDdl.users(tEnv);

        tEnv.createTemporarySystemFunction("MASK_EMAIL", MaskEmail.class);
        tEnv.createTemporarySystemFunction("SPLIT_INTERESTS", SplitInterests.class);
        tEnv.createTemporarySystemFunction("TOP_INTEREST", TopInterest.class);

        tEnv.toChangelogStream(tEnv.sqlQuery(
                        "SELECT user_id, UPPER(full_name), MASK_EMAIL(email), country FROM users"))
                .sinkTo(new H2ChangelogSink(USERS, List.of("user_id", "name_upper", "masked_email", "country"),
                        List.of("user_id")))
                .name("h2:" + USERS);

        tEnv.toChangelogStream(tEnv.sqlQuery("""
                        SELECT u.user_id, t.interest
                        FROM users AS u, LATERAL TABLE(SPLIT_INTERESTS(u.interests)) AS t(interest)"""))
                .sinkTo(new H2ChangelogSink(INTERESTS, List.of("user_id", "interest"), List.of("user_id", "interest")))
                .name("h2:" + INTERESTS);

        tEnv.toChangelogStream(tEnv.sqlQuery("""
                        SELECT u.country, COUNT(DISTINCT u.user_id) AS users, TOP_INTEREST(t.interest) AS top_interest
                        FROM users AS u, LATERAL TABLE(SPLIT_INTERESTS(u.interests)) AS t(interest)
                        GROUP BY u.country"""))
                .sinkTo(new H2ChangelogSink(COUNTRIES, List.of("country", "users", "top_interest"), List.of("country")))
                .name("h2:" + COUNTRIES);

        env.execute("uc12-udfs");
    }

    /** alice@example.com -> a****@example.com */
    public static class MaskEmail extends ScalarFunction {
        public String eval(String email) {
            if (email == null || !email.contains("@")) {
                return email;
            }
            int at = email.indexOf('@');
            return email.charAt(0) + "*".repeat(at - 1) + email.substring(at);
        }
    }

    /** "music;sports" -> rows ("music"), ("sports") */
    @FunctionHint(output = @DataTypeHint("ROW<interest STRING>"))
    public static class SplitInterests extends TableFunction<Row> {
        public void eval(String interests) {
            if (interests == null) {
                return;
            }
            for (String interest : interests.split(";")) {
                if (!interest.isBlank()) {
                    collect(Row.of(interest.trim()));
                }
            }
        }
    }

    /** Most frequent value in the group; ties go to the alphabetically first value. */
    public static class TopInterest extends AggregateFunction<String, TopInterest.Acc> {

        /** Accumulator: a POJO, so Flink derives its type and checkpoints it. */
        public static class Acc {
            public Map<String, Integer> counts = new HashMap<>();
        }

        @Override
        public Acc createAccumulator() {
            return new Acc();
        }

        public void accumulate(Acc acc, String interest) {
            if (interest != null) {
                acc.counts.merge(interest, 1, Integer::sum);
            }
        }

        @Override
        public String getValue(Acc acc) {
            return acc.counts.entrySet().stream()
                    .min(Map.Entry.<String, Integer>comparingByValue().reversed().thenComparing(Map.Entry.comparingByKey()))
                    .map(Map.Entry::getKey)
                    .orElse(null);
        }
    }
}
