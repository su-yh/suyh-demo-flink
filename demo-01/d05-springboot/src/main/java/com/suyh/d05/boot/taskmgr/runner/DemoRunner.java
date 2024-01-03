package com.suyh.d05.boot.taskmgr.runner;

import com.suyh.d05.boot.taskmgr.entity.UserEntity;
import com.suyh.d05.boot.taskmgr.mapper.UserMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author suyh
 * @since 2023-12-22
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DemoRunner {
    private final UserMapper userMapper;

    @PostConstruct
    public void run() throws Exception {
        log.info("DemoRunner run...");
        System.out.println("DemoRunner run...");

        List<UserEntity> userEntities = userMapper.selectList(null);
        if (userEntities == null || userEntities.isEmpty()) {
            System.out.println("userEntities is empty.");
            log.info("userEntities is empty.");
        } else {
            log.info("userEntities size: {}.", userEntities.size());
            for (UserEntity userEntity : userEntities) {
                log.info("userEntity info: {}", userEntity);
            }
        }
    }

    public void showHello() {
        log.info("hello spring boot DemoRunner");
    }
}
