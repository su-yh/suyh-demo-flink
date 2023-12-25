package com.suyh.d06.task.boot;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

/**
 * @author suyh
 * @since 2023-12-22
 */
@EnableCaching(proxyTargetClass = true)
@SpringBootApplication
public class DemoApplication {
}
