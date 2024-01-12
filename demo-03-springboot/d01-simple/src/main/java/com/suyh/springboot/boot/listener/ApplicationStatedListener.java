package com.suyh.springboot.boot.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.suyh.springboot.util.JsonUtils;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.lang.NonNull;

/**
 * @author suyh
 * @since 2024-01-12
 */
public class ApplicationStatedListener implements ApplicationListener<ApplicationStartedEvent> {
    @Override
    public void onApplicationEvent(@NonNull ApplicationStartedEvent event) {
        ConfigurableApplicationContext context = event.getApplicationContext();
        ObjectMapper objectMapper = context.getBean(ObjectMapper.class);
        JsonUtils.initMapper(objectMapper);
    }
}
