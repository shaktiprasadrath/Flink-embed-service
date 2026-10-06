package com.shakti.flinkdemo.common;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/**
 * Watermark strategies for the demos.
 *
 * <p>Flink's built-in {@code forBoundedOutOfOrderness} emits watermarks periodically (every 200 ms by
 * default). With small files the whole input is read before the first periodic watermark, so results
 * would depend on timing. The demos therefore use a punctuated variant that emits a watermark after
 * every event: same semantics, deterministic output.
 */
public final class Watermarks {

    private Watermarks() {}

    public static <T> WatermarkStrategy<T> perEvent(Duration maxOutOfOrderness, SerializableTimestampAssigner<T> timestamps) {
        long delay = maxOutOfOrderness.toMillis();
        return WatermarkStrategy.<T>forGenerator(ctx -> new PerEventBoundedOutOfOrderness<>(delay))
                .withTimestampAssigner(timestamps);
    }

    /** Same contract as BoundedOutOfOrdernessWatermarks, but emits on every event. */
    static final class PerEventBoundedOutOfOrderness<T> implements WatermarkGenerator<T> {
        private final long delay;
        private long maxTimestamp;

        PerEventBoundedOutOfOrderness(long delay) {
            this.delay = delay;
            this.maxTimestamp = Long.MIN_VALUE + delay + 1;
        }

        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
            output.emitWatermark(new Watermark(maxTimestamp - delay - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // all watermarks are emitted in onEvent
        }
    }
}
