package com.aiteer.springboot.common.vo;

import lombok.Data;

import java.util.concurrent.TimeUnit;

/**
 * @author suyh
 * @since 2024-01-22
 */
@Data
public class CohortThreadPoolProperties {
    private Integer corePoolSize = 4;
    private Integer maximumPoolSize = 32;
    private Long keepAliveTime = 60L;
    private TimeUnit unit = TimeUnit.SECONDS;
    private Integer workQueue = Integer.MAX_VALUE;
}
