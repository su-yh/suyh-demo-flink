package com.suyh.d02.property.context;

import com.suyh.d02.property.environment.FlinkSpringbootEnvironmentPostProcessor;
import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.lang.Nullable;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author suyh
 * @since 2024-01-03
 */
@SpringBootApplication
public class FlinkSpringContext {
    private static volatile ConfigurableApplicationContext context;
    private static final ReentrantLock LOCK = new ReentrantLock();

    // 允许调用者指定注解(@SpringBootApplication) 的类
    public static void init(@Nullable Class<?> primarySource, String[] args, Map<String, Object> configProperties) {
        if (context == null) {
            try {
                LOCK.lock();
                if (context == null) {
                    FlinkSpringbootEnvironmentPostProcessor.setConfigProperties(configProperties);
                    if (primarySource == null) {
                        primarySource = FlinkSpringContext.class;
                    }
                    context = SpringApplication.run(primarySource, args);
                }
            } finally {
                LOCK.unlock();
            }
        }
    }

    public static void closeContext() {
        if (context != null) {
            try {
                LOCK.lock();
                if (context != null) {
                    context.close();
                    context = null;
                }
            } finally {
                LOCK.unlock();
            }
        }
    }

    public static ConfigurableApplicationContext getContext() {
        return context;
    }

    public static <T> T getBean(Class<T> requiredType) throws BeansException {
        return context.getBean(requiredType);
    }
}
