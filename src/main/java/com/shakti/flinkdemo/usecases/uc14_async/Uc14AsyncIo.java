package com.shakti.flinkdemo.usecases.uc14_async;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.Sources;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.model.Txn;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * UC-14: Async I/O. Enriches each transaction with the account's risk tier from a (simulated)
 * remote service without blocking the operator: up to 10 requests are in flight at once.
 * <ul>
 *   <li>orderedWait: results leave in input order even though later requests finish first
 *       (earlier transactions are slowed down on purpose)</li>
 *   <li>retry strategy: the service fails the first call for account A2; Flink retries it</li>
 *   <li>timeout: requests taking more than 5 s fail the job (override {@code timeout()} to fall back)</li>
 * </ul>
 */
public class Uc14AsyncIo implements UseCase {

    public static final String TABLE = "async_enriched";

    public record EnrichedTxn(String txnId, String accountId, double amount, String riskTier, int attempts) {}

    public record Sequenced(int seq, EnrichedTxn txn) {}

    @Override
    public String id() {
        return "uc14";
    }

    @Override
    public String title() {
        return "Async I/O: non-blocking enrichment, ordered results, retries";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TABLE);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(TABLE, "seq INT PRIMARY KEY, txn_id VARCHAR(10), account_id VARCHAR(10), amount DECIMAL(12,2), "
                + "risk_tier VARCHAR(10), attempts INT");

        StreamExecutionEnvironment env = Envs.local(1);

        // Flink stops retrying once the async operator's input has ended (bounded input). The file is
        // tiny, so the input is throttled to keep it open while retries happen, as in a real stream.
        DataStream<Txn> txns = Sources.lines(env, DataFiles.TXNS)
                .map(line -> {
                    Thread.sleep(100);
                    return Txn.parse(line);
                })
                .name("parse-txns-throttled");

        AsyncRetryStrategy<EnrichedTxn> retry = new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<EnrichedTxn>(3, 50L)
                .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
                .build();

        DataStream<EnrichedTxn> enriched = AsyncDataStream
                .orderedWaitWithRetry(txns, new RiskTierLookup(), 5, TimeUnit.SECONDS, 10, retry)
                .name("async-risk-lookup");

        enriched
                .map(new RichMapFunction<EnrichedTxn, Sequenced>() {
                    private int seq;

                    @Override
                    public Sequenced map(EnrichedTxn t) {
                        return new Sequenced(++seq, t); // order in which results left the async operator
                    }
                })
                .name("number-results")
                .sinkTo(JdbcSinks.sink("INSERT INTO " + TABLE + " VALUES (?, ?, ?, ?, ?, ?)", (ps, s) -> {
                    ps.setInt(1, s.seq());
                    ps.setString(2, s.txn().txnId());
                    ps.setString(3, s.txn().accountId());
                    ps.setBigDecimal(4, BigDecimal.valueOf(s.txn().amount()));
                    ps.setString(5, s.txn().riskTier());
                    ps.setInt(6, s.txn().attempts());
                }))
                .name("h2:" + TABLE);

        env.execute("uc14-async-io");
    }

    static class RiskTierLookup extends RichAsyncFunction<Txn, EnrichedTxn> {
        private static final Map<String, String> TIERS = Map.of("A1", "LOW", "A2", "MEDIUM", "A3", "HIGH");

        private transient ExecutorService executor;
        private transient Map<String, Integer> attempts;

        @Override
        public void open(OpenContext openContext) {
            executor = Executors.newFixedThreadPool(4);
            attempts = new ConcurrentHashMap<>();
        }

        @Override
        public void asyncInvoke(Txn txn, ResultFuture<EnrichedTxn> resultFuture) {
            int attempt = attempts.merge(txn.txnId(), 1, Integer::sum);
            CompletableFuture
                    .supplyAsync(() -> callRiskService(txn, attempt), executor)
                    .whenComplete((tier, error) -> {
                        if (error != null) {
                            resultFuture.completeExceptionally(error);
                        } else {
                            resultFuture.complete(List.of(new EnrichedTxn(txn.txnId(), txn.accountId(), txn.amount(),
                                    tier, attempt)));
                        }
                    });
        }

        /** Simulated remote call: A2's first call fails fast; earlier transactions answer slower. */
        private static String callRiskService(Txn txn, int attempt) {
            if (txn.accountId().equals("A2") && attempt == 1) {
                throw new IllegalStateException("risk service temporarily unavailable");
            }
            int n = Integer.parseInt(txn.txnId().substring(1));
            try {
                Thread.sleep((8 - n) * 40L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return TIERS.getOrDefault(txn.accountId(), "UNKNOWN");
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}
