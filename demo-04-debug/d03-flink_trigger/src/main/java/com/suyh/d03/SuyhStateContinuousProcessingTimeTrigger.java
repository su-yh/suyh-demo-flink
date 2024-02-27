package com.suyh.d03;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 使用state 来记录定时器的时间戳
 *
 * @author suyh
 * @since 2024-01-31
 */
@Slf4j
public class SuyhStateContinuousProcessingTimeTrigger extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private final long interval;

    // TODO: suyh - 为什么使用它就可以解决问题了呢？
    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);

    private SuyhStateContinuousProcessingTimeTrigger(long interval) {
        this.interval = interval;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx)
            throws Exception {
        ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);

        timestamp = ctx.getCurrentProcessingTime();

        if (fireTimestampState.get() == null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String windowStart = sdf.format(new Date(window.getStart()));
            String windowEnd = sdf.format(new Date(window.getEnd()));
            log.info("onElement init register next fire timestamp. windowStart: {}, windowEnd: {}", windowStart, windowEnd);
            long nextFireTimestamp = timestamp - (timestamp % interval) + interval;
            registerNextFireTimestamp(nextFireTimestamp, window, ctx, fireTimestampState);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String windowStart = sdf.format(new Date(window.getStart()));
        String windowEnd = sdf.format(new Date(window.getEnd()));


        ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);
        long currentProcessingTime = ctx.getCurrentProcessingTime();

        if (currentProcessingTime >= fireTimestampState.get()) {
            // 触发当前计算
            log.info("TriggerResult.FIRE. windowStart: {}, windowEnd: {}.", windowStart, windowEnd);
            fireTimestampState.clear();
            long nextFireTimestamp = currentProcessingTime - currentProcessingTime % interval + interval;
            registerNextFireTimestamp(nextFireTimestamp, window, ctx, fireTimestampState);
            return TriggerResult.FIRE;
        }

        String fireTimestampStateValue = sdf.format(new Date(fireTimestampState.get()));
        String currentProcessingTimeValue = sdf.format(new Date(currentProcessingTime));
        log.error("TriggerResult.CONTINUE. windowStart: {}, windowEnd: {}. fireTimestampState: {}, current processing time: {}",
                windowStart, windowEnd, fireTimestampStateValue, currentProcessingTimeValue);
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        Long timestamp = fireTimestamp.get();
        if (timestamp != null) {
            ctx.deleteProcessingTimeTimer(timestamp);
            fireTimestamp.clear();
        }
    }

    public static SuyhStateContinuousProcessingTimeTrigger of(Time interval) {
        return new SuyhStateContinuousProcessingTimeTrigger(interval.toMilliseconds());
    }

    private void registerNextFireTimestamp(long nextFireTimestamp, TimeWindow window, TriggerContext ctx, ReducingState<Long> fireTimestampState) throws Exception {
        // 如果期望注册的时间超过了，窗口的最大时间，则取窗口的最大时间戳
        nextFireTimestamp = Math.min(nextFireTimestamp, window.maxTimestamp());
        fireTimestampState.add(nextFireTimestamp);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String formatTime = sdf.format(new Date(nextFireTimestamp));
        log.info("register next fire timestamp: {}, formatTime: {}", nextFireTimestamp, formatTime);
        ctx.registerProcessingTimeTimer(nextFireTimestamp);
    }

    private static class Min implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }

}
