package com.leomaster.utils;

import com.aiteer.springboot.common.vo.RmqConnectProperties;
import com.alibaba.fastjson.JSONObject;
import com.leomaster.core.util.TextUtils;
import com.leomaster.utils.mq.sink.MqDataSinkFunction;
import com.rabbitmq.client.AMQP;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * Desc: 操作RabbitMq的工具类
 */
public class MyRMQUtil {

    private final static Logger LOGGER = LoggerFactory.getLogger(MyRMQUtil.class);

    public static class RichRMQSink<IN> extends RMQSink<IN> {

        private RMQConnectionConfig rmqConnectionConfigRef;

        private boolean logFailuresOnly = false;

        public RichRMQSink(RMQConnectionConfig rmqConnectionConfig, String queueName, SerializationSchema<IN> schema) {
            super(rmqConnectionConfig, queueName, schema);
            rmqConnectionConfigRef = rmqConnectionConfig;
        }

        /**
         * 此方法必须重写，如果RabbitMQ的queue的durable属性设置为true，则会导致RabbitMQ会一直connection，导致RabbitMQ耗尽资源挂掉
         */
        @Override
        protected void setupQueue() throws IOException {
            this.channel.queueDeclare(this.queueName, true, false, false, null);
        }

        private String parseCorrelationIdFromMsg(byte[] msg) {
            try {
                JSONObject messageObject = JSONObject.parseObject(new String(msg));
                return messageObject.getString("correlationId");
            } catch (Exception e) {
                LOGGER.error("[Exception]", e);
            }
            return "";
        }

        @Override
        public void invoke(IN value) {
            try {
                byte[] msg = this.schema.serialize(value);

                //这里优先从message里面获取已经生成的correlationId，因为上游数据重放的时候，会再次往队列中写入转换后的数据，后面可能需要
                //利用correlationId来做数据去重
                String correlationId = parseCorrelationIdFromMsg(msg);
                if (TextUtils.isEmpty(correlationId)) {
                    correlationId = UUID.randomUUID().toString();
                } else {
                    LOGGER.debug("use existed correlationId from message");
                }
                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .correlationId(correlationId).build();

                this.channel.basicPublish("", this.queueName, properties, msg);
            } catch (IOException var3) {
                if (!this.logFailuresOnly) {
                    throw new RuntimeException("Cannot send RMQ message " + this.queueName + " at " + this.rmqConnectionConfigRef.getHost(), var3);
                }
                LOGGER.error("Cannot send RMQ message {} at {}", this.queueName, this.rmqConnectionConfigRef.getHost(), var3);
            }
        }
    }


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

    public static RMQSource<String> buildRmqSource(
            RmqConnectProperties rmqSource, String queueName, Integer prefetchCount) {
        return buildRmqSource(rmqSource, queueName, true, prefetchCount);
    }

    public static RMQSource<String> buildRmqSource(
            RmqConnectProperties rmqSource, String queueName, boolean usesCorrelationId, Integer prefetchCount) {
        RMQConnectionConfig rmqConfig = buildRmqConfig(rmqSource, prefetchCount);
        return new RMQSource<>(rmqConfig, queueName, usesCorrelationId, new org.apache.flink.api.common.serialization.SimpleStringSchema());
    }

    public static MyRMQUtil.RichRMQSink<String> buildRmqSink(RmqConnectProperties rmqSource, String queueName) {
        RMQConnectionConfig rmqConfig = buildRmqConfig(rmqSource, null);
        return new MyRMQUtil.RichRMQSink<>(rmqConfig, queueName, new org.apache.flink.api.common.serialization.SimpleStringSchema());
    }

    public static MqDataSinkFunction buildRmqSink(RmqConnectProperties rmqConnectProperties, String exchange, String routingKey) {
        RMQConnectionConfig rmqConfig = buildRmqConfig(rmqConnectProperties, null);
        return new MqDataSinkFunction(rmqConfig, exchange, routingKey);
    }

}
