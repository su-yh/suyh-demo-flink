package com.leomaster.utils.mq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class DataRichSinkFunction<IN> extends RichSinkFunction<IN> {

    protected final RMQConnectionConfig rmqConnectionConfig;
    protected final String exchange;

    protected Connection connection;

    protected Channel channel;

    public DataRichSinkFunction(RMQConnectionConfig rmqConnectionConfig, String exchange){
        this.rmqConnectionConfig = rmqConnectionConfig;
        this.exchange = exchange;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(rmqConnectionConfig.getHost());
        connectionFactory.setPort(rmqConnectionConfig.getPort());
        connectionFactory.setUsername(rmqConnectionConfig.getUsername());
        connectionFactory.setPassword(rmqConnectionConfig.getPassword());
        connectionFactory.setVirtualHost(rmqConnectionConfig.getVirtualHost());
        connection = connectionFactory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC,true);
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.channel.close();
        this.connection.close();
    }
}
