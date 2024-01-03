package com.suyh.d05.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author suyh
 * @since 2023-12-22
 */
@SpringBootApplication
public class DemoApplication {

    private static ConfigurableApplicationContext context;
    private static final ReentrantLock LOCK = new ReentrantLock();

    public static ConfigurableApplicationContext getContext(String[] args) {
        if (context == null) {
            try {
                LOCK.lock();
                if (context == null) {
                    context = SpringApplication.run(DemoApplication.class, args);
                }
            } finally {
                LOCK.unlock();
            }
        }

        return context;
    }
}
