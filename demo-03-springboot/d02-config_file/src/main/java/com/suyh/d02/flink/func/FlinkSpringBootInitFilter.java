package com.suyh.d02.flink.func;

import com.suyh.d02.flink.vo.FlinkUserEntity;
import com.suyh.d02.springboot.taskmgr.TaskManagerSpringContext;
import com.suyh.d02.springboot.util.JsonUtils;
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
    public FlinkSpringBootInitFilter(Map<String, Object> configProperties) {
        System.out.println("spring boot init: " + FlinkSpringBootInitFilter.class.getSimpleName());
        // TODO: suyh - 还没找到TaskManager 只做一次初始化的地方
        //  因为在这里调用的话，每一个并行度都会调用一次。
        TaskManagerSpringContext.init(new String[0], configProperties);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        FlinkUserEntity flinkUser = new FlinkUserEntity();
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
