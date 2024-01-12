package com.suyh.d02.springboot.taskmgr;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author suyh
 * @since 2024-01-03
 */
@SpringBootApplication
public class TaskManagerSpringContext {
    private static ConfigurableApplicationContext context;
    private static final ReentrantLock LOCK = new ReentrantLock();

    public static ConfigurableApplicationContext getContext(String[] args) {
        if (context == null) {
            try {
                LOCK.lock();
                if (context == null) {
                    context = SpringApplication.run(TaskManagerSpringContext.class, args);
                }
            } finally {
                LOCK.unlock();
            }
        }

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
