package com.suyh.d06.boot.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.Date;

/**
 * @author suyh
 * @since 2023-12-23
 */
@Data
@TableName("`user`")
public class UserEntity {
    @TableId
    private Long id;
    private String name;
    private Integer age;
    private String email;
    private Date createDate;
}
