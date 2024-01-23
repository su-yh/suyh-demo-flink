package com.suyh.d02.app.vo;

import lombok.Data;

import java.util.Date;

/**
 *
 * @author suyh
 * @since 2023-12-23
 */
@Data
public class FlinkUserTestEntity {
    private Long id;

    private String username;
    private Integer age;
    private String email;
    private Date createDate;
}
