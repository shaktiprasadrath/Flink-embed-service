package com.shakti.flinkdemo;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.usecases.UseCases;
import org.h2.tools.Server;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Command line entry point.
 *
 * <pre>
 *   ./gradlew run                      # list use cases
 *   ./gradlew run --args="uc01"        # run one use case
 *   ./gradlew run --args="uc01 uc09"   # run several
 *   ./gradlew run --args="all"         # run every use case
 * </pre>
 */
public final class App {

    private App() {}

    public static void main(String[] args) {
        List<String> ids = Arrays.stream(args).flatMap(a -> Arrays.stream(a.split(","))).map(String::trim)
                .filter(a -> !a.isEmpty()).toList();
        if (ids.isEmpty() || ids.contains("list")) {
            printList();
            return;
        }

        List<UseCase> selected = new ArrayList<>();
        for (String id : ids) {
            if (id.equalsIgnoreCase("all")) {
                selected.addAll(UseCases.ALL);
            } else {
                selected.add(UseCases.byId(id).orElseThrow(() ->
                        new IllegalArgumentException("Unknown use case '" + id + "'. Run without args to list.")));
            }
        }

        List<String> failed = new ArrayList<>();
        for (UseCase uc : selected) {
            System.out.println();
            System.out.println("=== " + uc.id() + ": " + uc.title() + " ===");
            long start = System.currentTimeMillis();
            try {
                uc.run();
                System.out.println("  finished in " + (System.currentTimeMillis() - start) + " ms");
                for (String table : uc.outputTables()) {
                    Db.print(table);
                }
            } catch (Exception e) {
                failed.add(uc.id());
                System.err.println("  FAILED: " + e);
                e.printStackTrace();
            }
        }

        System.out.println();
        System.out.println(failed.isEmpty()
                ? "All " + selected.size() + " use case(s) succeeded."
                : "Failed use cases: " + failed);
        boolean keepAlive = Boolean.getBoolean("flinkdemo.keepalive");
        if (Boolean.getBoolean("flinkdemo.h2.console") || keepAlive) {
            openH2Console(keepAlive);
        }
        // Flink and H2 leave non-daemon threads; exit explicitly.
        System.exit(failed.isEmpty() ? 0 : 1);
    }

    /**
     * The DB lives in this JVM only, so keep the JVM alive while the console is in use: until Enter
     * is pressed, or (flinkdemo.keepalive=true, e.g. in a container) until the process is stopped.
     */
    private static void openH2Console(boolean keepAlive) {
        try {
            List<String> options = new ArrayList<>(List.of("-webPort", "8082"));
            // By default H2 only accepts connections from this machine. A container needs remote access
            // (docker -p / kubectl port-forward); the console has no login, so never expose it publicly.
            if (Boolean.getBoolean("flinkdemo.h2.allowOthers")) {
                options.add("-webAllowOthers");
            }
            Server web = Server.createWebServer(options.toArray(String[]::new)).start();
            System.out.println();
            System.out.println("H2 console: http://localhost:8082  (JDBC URL " + Db.URL + ", user " + Db.USER
                    + ", empty password)");
            if (keepAlive) {
                System.out.println("Keeping the JVM alive until it is stopped.");
                Thread.currentThread().join();
            }
            System.out.println("Press Enter to stop.");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            web.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("Could not start H2 console: " + e);
        }
    }

    private static void printList() {
        System.out.println("Usage: ./gradlew run --args=\"<id> [<id> ...] | all\"");
        System.out.println();
        for (UseCase uc : UseCases.ALL) {
            System.out.printf("  %-5s %s%n", uc.id(), uc.title());
        }
    }
}
