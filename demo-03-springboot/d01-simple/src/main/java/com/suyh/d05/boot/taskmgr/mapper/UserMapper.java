package com.suyh.d05.boot.taskmgr.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.suyh.d05.boot.taskmgr.entity.UserEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * @author suyh
 * @since 2023-12-23
 */
@Mapper
public interface UserMapper extends BaseMapper<UserEntity> {
}
