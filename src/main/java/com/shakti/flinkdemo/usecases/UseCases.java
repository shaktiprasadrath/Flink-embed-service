package com.shakti.flinkdemo.usecases;

import com.shakti.flinkdemo.common.UseCase;
import com.shakti.flinkdemo.usecases.uc01_basics.Uc01BasicTransformations;
import com.shakti.flinkdemo.usecases.uc02_eventtime.Uc02EventTimeWatermarks;
import com.shakti.flinkdemo.usecases.uc03_windows.Uc03Windows;
import com.shakti.flinkdemo.usecases.uc04_keyedstate.Uc04KeyedState;
import com.shakti.flinkdemo.usecases.uc05_broadcast.Uc05BroadcastState;
import com.shakti.flinkdemo.usecases.uc06_timers.Uc06ProcessFunctionTimers;
import com.shakti.flinkdemo.usecases.uc07_sideoutputs.Uc07SideOutputs;
import com.shakti.flinkdemo.usecases.uc08_joins.Uc08Joins;
import com.shakti.flinkdemo.usecases.uc09_sql.Uc09TableApiSql;
import com.shakti.flinkdemo.usecases.uc10_temporal.Uc10TemporalLookupJoins;
import com.shakti.flinkdemo.usecases.uc11_changelog.Uc11ChangelogConversion;
import com.shakti.flinkdemo.usecases.uc12_udf.Uc12UserDefinedFunctions;
import com.shakti.flinkdemo.usecases.uc13_cep.Uc13ComplexEventProcessing;
import com.shakti.flinkdemo.usecases.uc14_async.Uc14AsyncIo;
import com.shakti.flinkdemo.usecases.uc15_exactlyonce.Uc15CheckpointingExactlyOnce;
import com.shakti.flinkdemo.usecases.uc16_savepoints.Uc16SavepointsStateBackend;
import com.shakti.flinkdemo.usecases.uc17_batch.Uc17BatchExecutionMode;
import com.shakti.flinkdemo.usecases.uc18_parallelism.Uc18ParallelismChaining;
import com.shakti.flinkdemo.usecases.uc19_metrics.Uc19MetricsAccumulators;
import com.shakti.flinkdemo.usecases.uc20_customio.Uc20CustomSourceSink;
import com.shakti.flinkdemo.usecases.uc21_webui.Uc21WebUi;

import java.util.List;
import java.util.Optional;

/** Registry of all use cases, in run order. */
public final class UseCases {

    public static final List<UseCase> ALL = List.of(
            new Uc01BasicTransformations(),
            new Uc02EventTimeWatermarks(),
            new Uc03Windows(),
            new Uc04KeyedState(),
            new Uc05BroadcastState(),
            new Uc06ProcessFunctionTimers(),
            new Uc07SideOutputs(),
            new Uc08Joins(),
            new Uc09TableApiSql(),
            new Uc10TemporalLookupJoins(),
            new Uc11ChangelogConversion(),
            new Uc12UserDefinedFunctions(),
            new Uc13ComplexEventProcessing(),
            new Uc14AsyncIo(),
            new Uc15CheckpointingExactlyOnce(),
            new Uc16SavepointsStateBackend(),
            new Uc17BatchExecutionMode(),
            new Uc18ParallelismChaining(),
            new Uc19MetricsAccumulators(),
            new Uc20CustomSourceSink(),
            new Uc21WebUi()
    );

    private UseCases() {}

    public static Optional<UseCase> byId(String id) {
        return ALL.stream().filter(uc -> uc.id().equalsIgnoreCase(id)).findFirst();
    }
}
