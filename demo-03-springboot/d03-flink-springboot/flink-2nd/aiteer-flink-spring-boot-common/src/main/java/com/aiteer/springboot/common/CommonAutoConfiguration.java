package com.aiteer.springboot.common;

import com.aiteer.springboot.common.properties.CommonProperties;
import com.aiteer.springboot.common.vo.CohortThreadPoolProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author suyh
 * @since 2024-01-17
 */
@AutoConfiguration
@EnableConfigurationProperties(CommonProperties.class)
public class CommonAutoConfiguration {

    @Lazy
    @Bean(name = "cohortThreadPool")
    public ExecutorService cohortThreadPool(CommonProperties commonProperties) {
        CohortThreadPoolProperties cohortThreadPool = commonProperties.getCohortThreadPool();
        return new ThreadPoolExecutor(
                cohortThreadPool.getCorePoolSize(),
                cohortThreadPool.getMaximumPoolSize(),
                cohortThreadPool.getKeepAliveTime(),
                cohortThreadPool.getUnit(),
                new LinkedBlockingDeque<>(cohortThreadPool.getWorkQueue()));
    }
}
