package com.suyh.d03;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * @author suyh
 * @since 2024-01-31
 */
public class SuyhContinuousProcessingTimeTrigger<W extends Window> extends Trigger<Object, W> {
    private static final long serialVersionUID = 1L;

    private final long interval;
    private boolean init = false;
    private Long timerTimestamp = null;

    private SuyhContinuousProcessingTimeTrigger(long interval) {
        this.interval = interval;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx)
            throws Exception {
        // 虽然这里的触发器逻辑跟元素没有任何关系，但是这个触发器没有提供初始化方法，所以也就只能在这里进行初始化操作了。
        if (!init) {
            init = true;
            registerNextFireTimestamp(ctx);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    /**
     * 注册的定时器到了的实现逻辑
     * <p>
     * 1. 注册下一个定时器
     * <p>
     * 2. 触发当前窗口计算
     */
    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx)
            throws Exception {
        // 注册的定时器到了的实现逻辑
        // 注册下一个定时器
        registerNextFireTimestamp(ctx);

        // 触发当前计算
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        if (timerTimestamp == null) {
            return;
        }

        // 清理定时器
        ctx.deleteProcessingTimeTimer(timerTimestamp);
        timerTimestamp = null;
    }

    public static <W extends Window> SuyhContinuousProcessingTimeTrigger<W> of(Time interval) {
        return new SuyhContinuousProcessingTimeTrigger<>(interval.toMilliseconds());
    }

    private void registerNextFireTimestamp(TriggerContext ctx) {
        timerTimestamp = ctx.getCurrentProcessingTime() + interval;
        ctx.registerProcessingTimeTimer(timerTimestamp);
    }
}
