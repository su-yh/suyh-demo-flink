package com.suyh.d02.property.config;

import com.suyh.d02.property.test.component.TestPropertyComponent;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonProperties;
import org.springframework.context.annotation.Bean;

/**
 * @author suyh
 * @since 2024-01-13
 */
@AutoConfiguration
public class FlinkSpringbootAutoConfiguration {

    @Bean
    public TestPropertyComponent propertyComponent(JacksonProperties jacksonProperties) {
        return new TestPropertyComponent(jacksonProperties);
    }
}
