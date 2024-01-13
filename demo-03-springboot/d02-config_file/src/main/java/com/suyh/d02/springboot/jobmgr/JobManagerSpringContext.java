package com.suyh.d02.springboot.jobmgr;

import com.suyh.d02.property.environment.FlinkSpringbootEnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author suyh
 * @since 2024-01-12
 */
@SpringBootApplication
public class JobManagerSpringContext {
    private static ConfigurableApplicationContext context;
    private static final ReentrantLock LOCK = new ReentrantLock();

    public static void init(String[] args, Map<String, Object> configProperties) {
        if (context == null) {
            try {
                LOCK.lock();
                if (context == null) {
                    FlinkSpringbootEnvironmentPostProcessor.setConfigProperties(configProperties);
                    context = SpringApplication.run(JobManagerSpringContext.class, args);
                }
            } finally {
                LOCK.unlock();
            }
        }
    }

    public static ConfigurableApplicationContext getContext() {
        return context;
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
}
