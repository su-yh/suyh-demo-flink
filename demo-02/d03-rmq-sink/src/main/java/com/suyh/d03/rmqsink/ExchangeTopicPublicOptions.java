package com.suyh.d03.rmqsink;

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
}
