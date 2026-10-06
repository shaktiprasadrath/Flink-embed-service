package com.shakti.flinkdemo.usecases.uc20_customio;

import com.shakti.flinkdemo.common.DataFiles;
import com.shakti.flinkdemo.common.Db;
import com.shakti.flinkdemo.common.Envs;
import com.shakti.flinkdemo.common.UseCase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * UC-20: Custom connectors. {@link LinesSource} (Source API: enumerator + readers + splits) reads two
 * files in parallel, one file per split; {@link BufferedH2Sink} (Sink V2 API) writes every line with
 * its file name, line number and the writer subtask.
 */
public class Uc20CustomSourceSink implements UseCase {

    public static final String TABLE = "custom_io_demo";

    @Override
    public String id() {
        return "uc20";
    }

    @Override
    public String title() {
        return "Custom Source (FLIP-27 splits/enumerator/reader) and custom Sink V2";
    }

    @Override
    public List<String> outputTables() {
        return List.of(TABLE);
    }

    @Override
    public void run() throws Exception {
        Db.recreate(TABLE, "file_name VARCHAR(40), line_no INT, line VARCHAR(200), writer_subtask INT, "
                + "PRIMARY KEY (file_name, line_no)");

        StreamExecutionEnvironment env = Envs.local(2);

        env.fromSource(new LinesSource(List.of(DataFiles.path(DataFiles.SENSORS), DataFiles.path(DataFiles.USERS))),
                        WatermarkStrategy.noWatermarks(), "custom-lines-source")
                .sinkTo(new BufferedH2Sink(TABLE, 5))
                .name("custom-h2-sink");

        env.execute("uc20-custom-source-sink");
    }
}
