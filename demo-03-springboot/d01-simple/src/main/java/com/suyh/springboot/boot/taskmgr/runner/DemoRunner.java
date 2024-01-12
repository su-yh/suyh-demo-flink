package com.suyh.springboot.boot.taskmgr.runner;

import com.suyh.springboot.boot.taskmgr.entity.FlinkUserEntity;
import com.suyh.springboot.boot.taskmgr.mapper.FlinkUserMapper;
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
    private final FlinkUserMapper flinkUserMapper;

    @PostConstruct
    public void run() throws Exception {
        log.info("DemoRunner run...");
        System.out.println("DemoRunner run...");

        List<FlinkUserEntity> userEntities = flinkUserMapper.selectList(null);
        if (userEntities == null || userEntities.isEmpty()) {
            System.out.println("userEntities is empty.");
            log.info("userEntities is empty.");
        } else {
            log.info("userEntities size: {}.", userEntities.size());
            for (FlinkUserEntity flinkUserEntity : userEntities) {
                log.info("userEntity info: {}", flinkUserEntity);
            }
        }
    }

    public void showHello() {
        log.info("hello spring boot DemoRunner");
    }
}
