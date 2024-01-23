package com.leomaster.utils;

import com.aiteer.springboot.common.vo.RmqConnectProperties;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class MyRMQUtil {
    public static RMQConnectionConfig buildRmqConfig(
            RmqConnectProperties connectProperties, Integer prefetchCount) {
        RMQConnectionConfig.Builder builder = new RMQConnectionConfig.Builder();
        builder.setHost(connectProperties.getHost())
                .setPort(connectProperties.getPort())
                .setUserName(connectProperties.getUserName())
                .setPassword(connectProperties.getPassword())
                .setVirtualHost(connectProperties.getVirtualHost());
        if (prefetchCount != null) {
            builder.setPrefetchCount(prefetchCount);
        }
        return builder.build();
    }
}
