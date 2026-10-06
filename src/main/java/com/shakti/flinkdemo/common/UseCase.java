package com.shakti.flinkdemo.common;

import java.util.List;

/** One runnable Flink feature demo: read a data file, process it, persist results to H2. */
public interface UseCase {

    /** Short id used on the command line, e.g. "uc01". */
    String id();

    String title();

    /** H2 tables this use case writes; printed after the run. */
    List<String> outputTables();

    /** Creates the output tables, builds the Flink job and runs it to completion. */
    void run() throws Exception;
}
