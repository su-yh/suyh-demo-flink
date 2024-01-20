package com.suyh.d02.app.func;

import com.suyh.d02.flink.springboot.core.context.FlinkSpringContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

/**
 * 专门来处理在TaskManager 中对springboot 的初始化操作。
 *
 * @author suyh
 * @since 2024-01-12
 * @deprecated 不能这样处理，因为flink 对于每一个算子被分配在哪一个TaskManager 进程是不确定的，有可能前面一部分算子链分配 在一个进程，而后面一部分算子链分配 在另一个进程。所以springboot 的初始化要放在需要使用spring boot 的对应 算子的open 和close 方法里面。
 */
@Deprecated
@Slf4j
public class FlinkSpringBootInitFilter<T> extends RichFilterFunction<T> {
    private static final long serialVersionUID = -336842266457209033L;

    private final Map<String, Object> configMap;

    public FlinkSpringBootInitFilter(Map<String, Object> configMap) {
        // TODO: suyh - 不能在这里进行springboot 的初始化，因为它的实例化也是在 flink client 做的
        //  然后对象会被传输到 task manager
        this.configMap = configMap;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        log.info("spring boot init: " + FlinkSpringBootInitFilter.class.getSimpleName());

        // TODO: suyh - 还没找到TaskManager 只做一次初始化的地方
        //  因为在这里调用的话，每一个并行度都会调用一次。
        FlinkSpringContext.init(configMap);
    }

    @Override
    public void close() throws Exception {
        super.close();

        FlinkSpringContext.closeContext();
    }

    @Override
    public boolean filter(T value) throws Exception {
        return true;
    }
}
