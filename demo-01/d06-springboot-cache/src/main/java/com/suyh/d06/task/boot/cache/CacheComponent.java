package com.suyh.d06.task.boot.cache;

import com.suyh.d06.task.boot.entity.UserEntity;
import com.suyh.d06.task.boot.mapper.UserMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

/**
 * @author suyh
 * @since 2023-12-25
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class CacheComponent {
    private final UserMapper userMapper;

    @Cacheable(cacheNames = "suyh-default", key = "#id", unless = "#result == null")
    public UserEntity queryById(Long id) {
        UserEntity userEntity = userMapper.selectById(id);
        log.info("query from db, id: {}, userEntity: {}", id, userEntity);
        return userEntity;
    }
}
