package com.shakti.flinkdemo.common;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** File sources shared by the use cases. */
public final class Sources {

    private Sources() {}

    /** Bounded FileSource that reads a data file line by line. Skips blank lines and '#' comments. */
    public static FileSource<String> fileLines(String file) {
        return FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(DataFiles.uri(file))).build();
    }

    public static DataStream<String> lines(StreamExecutionEnvironment env, String file) {
        return env.fromSource(fileLines(file), WatermarkStrategy.noWatermarks(), "file:" + file)
                .uid("source-" + file)
                .setParallelism(1)
                .filter(Sources::isData)
                .name("skip-comments:" + file)
                .setParallelism(1);
    }

    /**
     * Reads, parses and timestamps a file of events. Parsing and watermark generation run with
     * parallelism 1 so watermarks follow file order exactly (deterministic demo output).
     */
    public static <T> SingleOutputStreamOperator<T> events(StreamExecutionEnvironment env, String file,
                                                           MapFunction<String, T> parser, Class<T> type,
                                                           WatermarkStrategy<T> watermarks) {
        return lines(env, file)
                .map(parser).returns(type).name("parse:" + file).setParallelism(1)
                .assignTimestampsAndWatermarks(watermarks).name("watermarks:" + file).setParallelism(1);
    }

    public static boolean isData(String line) {
        return !line.isBlank() && !line.startsWith("#");
    }
}
