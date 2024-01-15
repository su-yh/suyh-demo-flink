package com.suyh.d02.flink.func;

import com.suyh.d02.FlinkSpringBootConfigDataDemo;
import com.suyh.d02.flink.vo.FlinkUserTestEntity;
import com.suyh.d02.property.context.FlinkSpringContext;
import com.suyh.d02.property.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Date;
import java.util.Map;

/**
 * 专门来处理在TaskManager 中对springboot 的初始化操作。
 *
 * @author suyh
 * @since 2024-01-12
 */
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
        FlinkSpringContext.init(FlinkSpringBootConfigDataDemo.class, new String[0], configMap);

        // TODO: suyh - 这些是测试代码
        FlinkUserTestEntity flinkUser = new FlinkUserTestEntity();
        flinkUser.setId(1L).setAge(18).setEmail("su787910081@163.com").setCreateDate(new Date());

        String flinkUserJson = JsonUtils.serializable(flinkUser);
        log.info("flinkUserJson: {}", flinkUserJson);
        System.out.println("flinkUserJson: " + flinkUserJson);
    }

    @Override
    public boolean filter(T value) throws Exception {
        return true;
    }
}
