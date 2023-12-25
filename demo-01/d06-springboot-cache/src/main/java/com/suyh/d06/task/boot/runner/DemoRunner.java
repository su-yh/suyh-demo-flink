package com.suyh.d06.task.boot.runner;

import com.suyh.d06.task.boot.cache.CacheComponent;
import com.suyh.d06.task.boot.entity.UserEntity;
import com.suyh.d06.task.boot.mapper.UserMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @author suyh
 * @since 2023-12-22
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DemoRunner {
    private final UserMapper userMapper;
    private final CacheComponent cacheComponent;

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

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            private int count = 0;

            @Override
            public void run() {
                UserEntity userEntity = cacheComponent.queryById(1L);
                log.info("query from cacheComponent, id: {}, userEntity: {}", userEntity.getId(), userEntity);

                if (++count >= 20) {
                    timer.cancel();
                }
            }
        }, 10_000, 1000);
    }

    public void showHello() {
        log.info("hello spring boot DemoRunner");
    }
}
