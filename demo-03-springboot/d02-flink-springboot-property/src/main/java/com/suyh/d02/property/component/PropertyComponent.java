package com.suyh.d02.property.component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jackson.JacksonProperties;

/**
 * @author suyh
 * @since 2024-01-13
 */
@Slf4j
@RequiredArgsConstructor
public class PropertyComponent implements InitializingBean {
    @Value("${spring.profile.active:}")
    private String active;
    @Value("${logging.config:}")
    private String logConfig;

    private final JacksonProperties jacksonProperties;

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("spring.profile.active: {}", active);

        log.info("suyh - dateformat: {}", jacksonProperties.getDateFormat());
        log.info("suyh - timezone: {}", jacksonProperties.getTimeZone());
        log.info("suyh - logConfig: {}", logConfig);
    }
}
