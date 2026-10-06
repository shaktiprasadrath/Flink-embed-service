package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.usecases.uc21_webui.Uc21WebUi;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;

class Uc21WebUiTest {

    @Test
    void webUiRestApiReportsRunningJob() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort(); // free port, so the test does not clash with a running 8081
        }

        new Uc21WebUi(port, 3).run();

        String[] row = Db.rows("SELECT * FROM webui_jobs").get(0).split("\\|");
        assertThat(row[0]).isEqualTo("uc21-web-ui-demo");
        assertThat(row[1]).isEqualTo("RUNNING");
        assertThat(Long.parseLong(row[2])).isPositive();
    }
}
