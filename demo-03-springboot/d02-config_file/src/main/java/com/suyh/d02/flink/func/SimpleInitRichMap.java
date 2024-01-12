package com.suyh.d02.flink.func;

import com.suyh.d02.flink.vo.FlinkUserEntity;
import com.suyh.d02.springboot.taskmgr.TaskManagerSpringContext;
import com.suyh.d02.springboot.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Date;

/**
 * @author suyh
 * @since 2024-01-12
 */
@Slf4j
public class SimpleInitRichMap<T> extends RichMapFunction<T, T> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // TODO: suyh - 还没找到taskmanager 只做一次初始化的地方
        //  因为在这里调用的话，每一个并行度都会调用一次。
        ConfigurableApplicationContext context = TaskManagerSpringContext.getContext(new String[0]);

        FlinkUserEntity flinkUser = new FlinkUserEntity();
        flinkUser.setId(1L).setAge(18).setEmail("su787910081@163.com").setCreateDate(new Date());

        String flinkUserJson = JsonUtils.serializable(flinkUser);
        log.info("flinkUserJson: {}", flinkUserJson);
    }

    @Override
    public T map(T value) throws Exception {
        // 直接返回原始数据
        return value;
    }
}
