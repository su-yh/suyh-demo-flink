package com.aiteer.springboot.core.listener;

import com.aiteer.springboot.core.environment.FlinkSpringbootEnvironmentPostProcessor;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.lang.NonNull;

/**
 * @author suyh
 * @since 2024-01-13
 */
public class DeferredLogListener implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {
    @Override
    public void onApplicationEvent(@NonNull ApplicationEnvironmentPreparedEvent event) {
        FlinkSpringbootEnvironmentPostProcessor.LOGGER.replayTo(FlinkSpringbootEnvironmentPostProcessor.class);
    }
}
