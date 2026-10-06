package com.shakti.flinkdemo.usecases.uc21_webui;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.JdbcSinks;
import com.shakti.flinkdemo.common.UseCase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * UC-21: Local execution with the Flink Web UI and REST API.
 *
 * <p>Starts an embedded cluster with the web dashboard (needs flink-runtime-web on the classpath),
 * runs an unbounded job (DataGeneratorSource, 5 records/s) for a while, and reads the job list
 * from the REST API. Open http://localhost:8081 while it runs to see the job graph, metrics,
 * checkpoints and back pressure.
 *
 * <p>System properties: {@code flinkdemo.webui.port} (default 8081),
 * {@code flinkdemo.webui.seconds} (default 30), {@code flinkdemo.webui.bind} (default localhost).
 */
public class Uc21WebUi implements UseCase {

    public static final String EVENTS = "webui_events";
    public static final String JOBS = "webui_jobs";

    private final int port;
    private final int seconds;

    public Uc21WebUi() {
        this(Integer.getInteger("flinkdemo.webui.port", 8081), Integer.getInteger("flinkdemo.webui.seconds", 30));
    }

    public Uc21WebUi(int port, int seconds) {
        this.port = port;
        this.seconds = seconds;
    }

    @Override
    public String id() {
        return "uc21";
    }

    @Override
    public String title() {
        return "Configuration + Web UI / REST API on a local cluster";
    }

    @Override
    public List<String> outputTables() {
        return List.of(JOBS);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(EVENTS, "seq BIGINT PRIMARY KEY");
        Db.recreate(JOBS, "job_name VARCHAR(100), job_state VARCHAR(20), events_written_so_far BIGINT");

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, port);
        // "0.0.0.0" makes the UI reachable from outside a container (docker -p, kubectl port-forward)
        conf.set(RestOptions.BIND_ADDRESS, System.getProperty("flinkdemo.webui.bind", "localhost"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);
        env.enableCheckpointing(2000); // so the Checkpoints tab has something to show

        DataGeneratorSource<Long> generator = new DataGeneratorSource<>(
                index -> index, Long.MAX_VALUE, RateLimiterStrategy.perSecond(5), Types.LONG);

        env.fromSource(generator, WatermarkStrategy.noWatermarks(), "generator")
                .sinkTo(JdbcSinks.sink("INSERT INTO " + EVENTS + " VALUES (?)", (ps, n) -> ps.setLong(1, n), 1, 0))
                .name("h2:" + EVENTS);

        JobClient job = env.executeAsync("uc21-web-ui-demo");
        String url = "http://localhost:" + port;
        System.out.println("  Web UI running at " + url + " for " + seconds + " s");
        try {
            Thread.sleep(Duration.ofSeconds(seconds));
            String overview = fetch(url + "/jobs/overview");
            String state = overview.contains("\"state\":\"RUNNING\"") ? "RUNNING" : "UNKNOWN";
            Db.execute("INSERT INTO " + JOBS + " VALUES ('uc21-web-ui-demo', '" + state + "', "
                    + Db.count(EVENTS) + ")");
        } finally {
            job.cancel().get(30, TimeUnit.SECONDS);
        }
    }

    static String fetch(String url) throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            return response.body();
        }
    }
}
