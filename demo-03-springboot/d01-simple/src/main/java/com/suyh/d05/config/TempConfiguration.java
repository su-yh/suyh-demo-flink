package com.suyh.d05.config;

import com.suyh.d05.component.SuyhComponent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author suyh
 * @since 2023-12-23
 */
@Configuration
public class TempConfiguration {
    @Bean
    public SuyhComponent suyhComponent() {
        return new SuyhComponent();
    }
}
