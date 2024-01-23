package com.suyh.d02.rmq.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * @author suyh
 * @since 2024-01-17
 */
public class SuyhRMQSource<T> extends RMQSource<T> {
    public SuyhRMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema<T> deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    public SuyhRMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, boolean usesCorrelationId, DeserializationSchema<T> deserializationSchema) {
        super(rmqConnectionConfig, queueName, usesCorrelationId, deserializationSchema);
    }

    public SuyhRMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, RMQDeserializationSchema<T> deliveryDeserializer) {
        super(rmqConnectionConfig, queueName, deliveryDeserializer);
    }

    public SuyhRMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, boolean usesCorrelationId, RMQDeserializationSchema<T> deliveryDeserializer) {
        super(rmqConnectionConfig, queueName, usesCorrelationId, deliveryDeserializer);
    }

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
