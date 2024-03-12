package com.suyh.cdc.rmqsink;

import com.rabbitmq.client.AMQP;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;

/**
 * @author suyh
 * @since 2024-02-21
 */
@Slf4j
public class ExchangeTopicPublicOptions<IN> implements RMQSinkPublishOptions<IN> {
    private static final long serialVersionUID = 2985210242585525978L;

    private final String exchange;
    private final String routingKey;

    public ExchangeTopicPublicOptions(String exchange, String routingKey) {
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    @Override
    public String computeRoutingKey(IN inValue) {
        log.info("in data: {}", inValue);
        return routingKey;
    }

    @Override
    public AMQP.BasicProperties computeProperties(IN inValue) {
        return null;
    }

    @Override
    public String computeExchange(IN inValue) {
        return exchange;
    }

    @Override
    public boolean computeMandatory(IN a) {
        // true: 如果exchange 根据自身类型和消息routingKey 无法找到一个合适的queue 存储消息，那么broker 会调用basic.return 方法将消息返还给生产者；
        // false: 如果出现上述情况，则broker 会直接将消息丢弃。
        // 也就是说，如果我们的routeKey 填写错误了，可以通过该标志，再加上 SerializableReturnListener 来获结果。
        return true;
    }
}
