package com.leomaster.batch.func;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RMQSender<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(RMQSender.class);
    protected final String exchange;
    protected final String queueName;
    protected final String routingKey;
    private final RMQConnectionConfig rmqConnectionConfig;
    protected transient Connection connection;
    protected transient Channel channel;
    protected SerializationSchema<IN> schema;
    private boolean logFailuresOnly = false;

    public RMQSender(RMQConnectionConfig rmqConnectionConfig, String exchange, String queueName, String routingKey, SerializationSchema<IN> schema) {
        this.rmqConnectionConfig = rmqConnectionConfig;
        this.exchange = ""; // TODO: suyh - 先兼容历史能力。
        this.queueName = queueName;
        this.routingKey = routingKey;
        this.schema = schema;
    }

    public void setLogFailuresOnly(boolean logFailuresOnly) {
        this.logFailuresOnly = logFailuresOnly;
    }

    public void open() throws Exception {
        ConnectionFactory factory = this.rmqConnectionConfig.getConnectionFactory();

        try {
            this.connection = factory.newConnection();
            this.channel = this.connection.createChannel();
//             this.channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC,true);
            // TODO: suyh - 原来的处理方式是直接使用队列，而未使用exchange，如果这里直接使用exchange 则可能下游需要做对应变更，所以先保留原来的队列能力。
            this.channel.queueDeclare(this.queueName, true, false, false, null);
            if (this.channel == null) {
                throw new RuntimeException("None of RabbitMQ channels are available");
            }
        } catch (IOException var4) {
            throw new RuntimeException("Error while creating the channel", var4);
        }
    }

    public void invoke(IN value) {
        try {
            byte[] msg = this.schema.serialize(value);
            this.channel.basicPublish(this.exchange, this.routingKey, null, msg);

            LOG.info("send RMQ message {} at {}", this.routingKey, this.rmqConnectionConfig.getHost());
        } catch (IOException var3) {
            if (!this.logFailuresOnly) {
                throw new RuntimeException("Cannot send RMQ message " + this.routingKey + " at " + this.rmqConnectionConfig.getHost(), var3);
            }

            LOG.error("Cannot send RMQ message {} at {}", this.routingKey, this.rmqConnectionConfig.getHost(), var3);
        }
    }

    public void close() {
        Exception t = null;

        try {
            this.channel.close();
        } catch (Exception var3) {
            t = var3;
        }

        try {
            this.connection.close();
        } catch (IOException var4) {
            if (t != null) {
                LOG.warn("Both channel and connection closing failed. Logging channel exception and failing with connection exception", t);
            }

            t = var4;
        }

        if (t != null) {
            throw new RuntimeException("Error while closing RMQ connection with " + this.queueName + " at " + this.rmqConnectionConfig.getHost(), t);
        }
    }
}
