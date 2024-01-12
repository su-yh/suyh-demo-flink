package com.suyh.springboot.boot.taskmgr.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.Date;

/**
 * @author suyh
 * @since 2023-12-23
 */
@Data
@TableName("flink_user")
public class FlinkUserEntity {
    @TableId(type = IdType.AUTO)
    private Long id;

    private String username;
    private Integer age;
    private String email;
    private Date createDate;
}
