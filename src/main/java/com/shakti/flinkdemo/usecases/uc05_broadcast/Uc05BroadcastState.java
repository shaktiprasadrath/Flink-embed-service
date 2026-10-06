package com.shakti.flinkdemo.usecases.uc05_broadcast;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.model.Rule;
import com.shakti.flinkdemo.model.Txn;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * UC-05: Broadcast state. Fraud rules (small, slowly changing) are broadcast to every parallel
 * instance; transactions (large, keyed by account) are checked against all rules.
 *
 * <p>Both inputs are independent, so a rule can arrive before or after a transaction. Each instance
 * therefore keeps the account's transactions in keyed state and, when a new rule arrives, re-checks
 * the stored transactions with {@code applyToKeyedState}. Every (transaction, rule) pair is evaluated
 * exactly once, whatever the arrival order.
 */
public class Uc05BroadcastState implements UseCase {

    public static final String TABLE = "flagged_txns";

    public record Flag(String txnId, String accountId, double amount, String ruleId) {}

    static final MapStateDescriptor<String, Rule> RULES =
            new MapStateDescriptor<>("rules", String.class, Rule.class);
    static final ListStateDescriptor<Txn> TXNS = new ListStateDescriptor<>("account-txns", Txn.class);

    @Override
    public String id() {
        return "uc05";
    }

    @Override
    public String title() {
        return "Broadcast state: dynamic fraud rules applied to keyed transactions";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TABLE);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(TABLE, "txn_id VARCHAR(10), account_id VARCHAR(10), amount DECIMAL(12,2), rule_id VARCHAR(10), "
                + "PRIMARY KEY (txn_id, rule_id)");

        StreamExecutionEnvironment env = Envs.local(2);

        DataStream<Txn> txns = Sources.lines(env, DataFiles.TXNS).map(Txn::parse).name("parse-txns");
        BroadcastStream<Rule> rules = Sources.lines(env, DataFiles.RULES).map(Rule::parse).name("parse-rules")
                .broadcast(RULES);

        txns.keyBy(Txn::accountId)
                .connect(rules)
                .process(new RuleEvaluator())
                .name("evaluate-rules")
                .sinkTo(JdbcSinks.sink("MERGE INTO " + TABLE + " KEY (txn_id, rule_id) VALUES (?, ?, ?, ?)", (ps, f) -> {
                    ps.setString(1, f.txnId());
                    ps.setString(2, f.accountId());
                    ps.setBigDecimal(3, BigDecimal.valueOf(f.amount()));
                    ps.setString(4, f.ruleId());
                }))
                .name("h2:" + TABLE);

        env.execute("uc05-broadcast-state");
    }

    static class RuleEvaluator extends KeyedBroadcastProcessFunction<String, Txn, Rule, Flag> {

        @Override
        public void processElement(Txn txn, ReadOnlyContext ctx, Collector<Flag> out) throws Exception {
            getRuntimeContext().getListState(TXNS).add(txn);
            ReadOnlyBroadcastState<String, Rule> rules = ctx.getBroadcastState(RULES);
            for (Map.Entry<String, Rule> rule : rules.immutableEntries()) {
                if (rule.getValue().matches(txn)) {
                    out.collect(new Flag(txn.txnId(), txn.accountId(), txn.amount(), rule.getKey()));
                }
            }
        }

        @Override
        public void processBroadcastElement(Rule rule, Context ctx, Collector<Flag> out) throws Exception {
            BroadcastState<String, Rule> rules = ctx.getBroadcastState(RULES);
            rules.put(rule.ruleId(), rule);
            // re-check transactions that arrived before this rule, for every key on this instance
            ctx.applyToKeyedState(TXNS, (account, state) -> {
                for (Txn txn : state.get()) {
                    if (rule.matches(txn)) {
                        out.collect(new Flag(txn.txnId(), txn.accountId(), txn.amount(), rule.ruleId()));
                    }
                }
            });
        }
    }
}
