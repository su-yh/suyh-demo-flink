package com.suyh.d06.component;

import javax.annotation.PostConstruct;

/**
 * @author suyh
 * @since 2023-12-23
 */
public class SuyhComponent {

    @PostConstruct
    public void init() {
        System.out.println("suyhComponent init");
    }

    public void showHello() {
        System.out.println("suyh Component hello.");
    }
}
