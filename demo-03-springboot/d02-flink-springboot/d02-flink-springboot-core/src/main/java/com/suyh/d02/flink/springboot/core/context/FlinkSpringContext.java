package com.suyh.d02.flink.springboot.core.context;

import com.suyh.d02.flink.springboot.core.environment.FlinkSpringbootEnvironmentPostProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.lang.Nullable;

import java.util.Map;

/**
 * @author suyh
 * @since 2024-01-03
 */
@SpringBootApplication
@Slf4j
public class FlinkSpringContext {
    private static volatile int REF_COUNT = 0;
    private static volatile ConfigurableApplicationContext context;

    public static void init(Map<String, Object> configProperties) {
        init(null, new String[0], configProperties);
    }

    // 允许调用者指定注解(@SpringBootApplication) 的类
    public static synchronized void init(@Nullable Class<?> primarySource, String[] args, Map<String, Object> configProperties) {
        int curCount = ++REF_COUNT;
        log.info("flink springboot init, current count: {}", curCount);
        if (context == null) {
            log.info("flink springboot starting...");
            FlinkSpringbootEnvironmentPostProcessor.setConfigProperties(configProperties);
            if (primarySource == null) {
                primarySource = FlinkSpringContext.class;
            }
            context = SpringApplication.run(primarySource, args);
            log.info("flink springboot started.");
        }
    }

    public static synchronized void closeContext() {
        int curCount = --REF_COUNT;
        log.info("flink spring close, current count: {}", curCount);
        if (curCount > 0) {
            return;
        }

        if (context != null) {
            context.close();
            context = null;
        }
        log.info("flink springboot closed.");
    }

    public static synchronized ConfigurableApplicationContext getContext() {
        return context;
    }

    public static synchronized <T> T getBean(Class<T> requiredType) throws BeansException {
        return context.getBean(requiredType);
    }
}
