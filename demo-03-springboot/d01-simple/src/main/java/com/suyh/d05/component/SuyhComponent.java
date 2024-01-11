package com.suyh.d05.component;

import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;

/**
 * @author suyh
 * @since 2023-12-23
 */
public class SuyhComponent {

    @Value("${log4j.configuration:}")
    private String log4jConfigFile;

    @PostConstruct
    public void init() {
        System.out.println("suyhComponent init");
        System.out.println("log4jConfigFile: " + log4jConfigFile);
    }

    public void showHello() {
        System.out.println("suyh Component hello.");
    }
}
