package com.suyh.d05.boot.runner;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author suyh
 * @since 2023-12-22
 */
@Component
@Slf4j
public class DemoRunner {
    @PostConstruct
    public void run() throws Exception {
        log.info("DemoRunner run...");
        System.out.println("DemoRunner run...");
    }

    public void showHello() {
        System.out.println("hello spring boot DemoRunner");
    }
}
