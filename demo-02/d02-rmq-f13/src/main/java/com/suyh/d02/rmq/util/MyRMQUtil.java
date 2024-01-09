package com.suyh.d02.rmq.util;

import com.rabbitmq.client.AMQP;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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

        @Override
        public void invoke(IN value) {
            try {
                byte[] msg = this.schema.serialize(value);

                //这里优先从message里面获取已经生成的correlationId，因为上游数据重放的时候，会再次往队列中写入转换后的数据，后面可能需要
                //利用correlationId来做数据去重
                String correlationId = UUID.randomUUID().toString();
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


    private final static RMQConnectionConfig CONNECTION_CONFIG = new RMQConnectionConfig.Builder()
            .setHost("139.9.50.208")
            .setPort(5672)
            .setUserName("admin")
            .setPassword("adminadmin")
            .setVirtualHost("/flinkhost")
            .setPrefetchCount(100)
            .setNetworkRecoveryInterval(30_000)
            .setConnectionTimeout(30_000)
            .setAutomaticRecovery(true)
            .build();

    public static RMQSource<String> getRMQSource(String queueName, boolean useCorrelationId) {
        return new RMQSource<>(CONNECTION_CONFIG, queueName, useCorrelationId, new SimpleStringSchema());
    }

    public static RMQSink<String> getRMQSink(String queueName) {
        return new RichRMQSink<>(CONNECTION_CONFIG, queueName, new SimpleStringSchema());
    }
}
