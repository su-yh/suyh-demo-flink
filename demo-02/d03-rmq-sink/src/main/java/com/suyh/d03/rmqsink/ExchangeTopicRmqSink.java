package com.suyh.d03.rmqsink;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;
import org.apache.flink.streaming.connectors.rabbitmq.SerializableReturnListener;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.IOException;

/**
 * @author suyh
 * @since 2024-02-21
 */
@Slf4j
public class ExchangeTopicRmqSink<IN> extends RMQSink<IN> {
    private static final long serialVersionUID = -4567791427676373093L;

    private final RMQSinkPublishOptions<IN> publishOptions;

    public ExchangeTopicRmqSink(
            RMQConnectionConfig rmqConnectionConfig, SerializationSchema<IN> schema,
            RMQSinkPublishOptions<IN> publishOptions) {
        super(rmqConnectionConfig, schema, publishOptions, new SerializableReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                // 如果监听要生效，则 mandatory 的值必须为true
                log.info("消息发送失败: handle return, replyCode: {}, replyText: {}, exchange: {}, routingKey: {}",
                        replyCode, replyText, exchange, routingKey);
                System.out.println(replyCode + ", " + replyText + ", " + exchange + ", " + routingKey);
            }
        });
        this.publishOptions = publishOptions;
    }

    @Override
    protected void setupQueue() throws IOException {
        // TODO: suyh - 主要就是提供给用户自定义的方法，用来声明queue。同时也可以声明 exchange
        String exchange = publishOptions.computeExchange(null);
        super.channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true);
    }

    @Override
    public void invoke(IN value) {
        log.info("invoke value: {}", value);
        System.out.println("value: " + value);
        super.invoke(value);
    }
}
