package com.suyh.d02.property.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.suyh.d02.property.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.lang.NonNull;

/**
 * @author suyh
 * @since 2024-01-12
 */
@Slf4j
public class ApplicationStatedListener implements ApplicationListener<ApplicationStartedEvent> {
    @Override
    public void onApplicationEvent(@NonNull ApplicationStartedEvent event) {
        System.out.println("suyh - [listener] ApplicationStatedListener");
        log.info("suyh - [listener] init object mapper.");
        ConfigurableApplicationContext context = event.getApplicationContext();
        ObjectMapper objectMapper = context.getBean(ObjectMapper.class);
        JsonUtils.initMapper(objectMapper);
    }
}
