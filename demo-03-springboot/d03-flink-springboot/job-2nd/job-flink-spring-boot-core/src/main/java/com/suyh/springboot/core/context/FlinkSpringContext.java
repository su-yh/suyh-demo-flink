package com.suyh.springboot.core.context;

import com.suyh.springboot.core.environment.FlinkSpringbootEnvironmentPostProcessor;
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
    private static int REFERENCE_COUNT = 0;
    private static volatile ConfigurableApplicationContext context;

    public static void init(Map<String, Object> configProperties) {
        init(null, new String[0], configProperties);
    }

    // 允许调用者指定注解(@SpringBootApplication) 的类
    public static synchronized void init(@Nullable Class<?> primarySource, String[] args, Map<String, Object> configProperties) {
        if (context == null) {
            log.info("flink springboot start.");
            FlinkSpringbootEnvironmentPostProcessor.setConfigProperties(configProperties);
            if (primarySource == null) {
                primarySource = FlinkSpringContext.class;
            }
            context = SpringApplication.run(primarySource, args);
        }
        int curCount = ++REFERENCE_COUNT;
        log.info("flink springboot init, current reference count: {}", curCount);
    }

    public static synchronized void closeContext() {
        int curCount = --REFERENCE_COUNT;
        log.info("flink springboot close, current reference count: {}", curCount);
        if (curCount > 0) {
            return;
        }

        if (context != null) {
            context.close();
            context = null;
            log.info("flink springboot closed.");
        }
    }

    public static synchronized ConfigurableApplicationContext getContext() {
        return context;
    }

    public static synchronized <T> T getBean(Class<T> requiredType) throws BeansException {
        return context.getBean(requiredType);
    }

    public static synchronized <T> T getBean(String name, Class<T> requiredType) throws BeansException {
        return context.getBean(name, requiredType);
    }
}
