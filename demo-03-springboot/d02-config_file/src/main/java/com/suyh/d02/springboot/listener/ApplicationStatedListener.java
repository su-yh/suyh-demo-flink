package com.suyh.d02.springboot.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.suyh.d02.springboot.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.lang.NonNull;

/**
 * @author suyh
 * @since 2024-01-12
 */
@Slf4j
public class ApplicationStatedListener implements ApplicationListener<ApplicationReadyEvent> {
    @Override
    public void onApplicationEvent(@NonNull ApplicationReadyEvent event) {
        System.out.println("suyh - [listener] ApplicationStatedListener");
        ConfigurableApplicationContext context = event.getApplicationContext();
        ObjectMapper objectMapper = context.getBean(ObjectMapper.class);
        JsonUtils.initMapper(objectMapper);
    }
}
