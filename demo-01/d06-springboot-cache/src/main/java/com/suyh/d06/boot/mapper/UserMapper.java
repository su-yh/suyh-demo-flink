package com.suyh.d06.boot.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.suyh.d06.boot.entity.UserEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * @author suyh
 * @since 2023-12-23
 */
@Mapper
public interface UserMapper extends BaseMapper<UserEntity> {
}
