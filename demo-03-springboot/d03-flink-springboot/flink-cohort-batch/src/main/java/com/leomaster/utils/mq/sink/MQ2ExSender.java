package com.leomaster.utils.mq.sink;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MQ2ExSender<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(MQ2ExSender.class);
    protected final String exchange;
    protected final String routingKey;
    private final RMQConnectionConfig rmqConnectionConfig;
    protected transient Connection connection;

    protected transient Channel channel;

    protected SerializationSchema<IN> schema;
    private boolean logFailuresOnly = false;

    public MQ2ExSender(RMQConnectionConfig rmqConnectionConfig,
                       String exchange,
                       String routingKey,
                       SerializationSchema<IN> schema) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.rmqConnectionConfig = rmqConnectionConfig;
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
            this.channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC,true);
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

            this.channel.basicPublish(exchange, routingKey, null, msg);

            LOG.info("send RMQ message {} at {}", exchange, this.rmqConnectionConfig.getHost());
        } catch (IOException var3) {
            if (!this.logFailuresOnly) {
                throw new RuntimeException("Cannot send RMQ message " + exchange + " at " + this.rmqConnectionConfig.getHost(), var3);
            }

            LOG.error("Cannot send RMQ message {} at {}", exchange, this.rmqConnectionConfig.getHost(), var3);
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
        } catch (Exception var4) {
            if (t != null) {
                LOG.warn("Both channel and connection closing failed. Logging channel exception and failing with connection exception", t);
            }

            t = var4;
        }

        if (t != null) {
            throw new RuntimeException("Error while closing RMQ connection with " + exchange + " at " + this.rmqConnectionConfig.getHost(), t);
        }
    }
}
