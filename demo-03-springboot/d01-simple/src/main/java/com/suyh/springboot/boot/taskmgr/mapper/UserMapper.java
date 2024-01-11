package com.suyh.springboot.boot.taskmgr.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.suyh.springboot.boot.taskmgr.entity.UserEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * @author suyh
 * @since 2023-12-23
 */
@Mapper
public interface UserMapper extends BaseMapper<UserEntity> {
}
