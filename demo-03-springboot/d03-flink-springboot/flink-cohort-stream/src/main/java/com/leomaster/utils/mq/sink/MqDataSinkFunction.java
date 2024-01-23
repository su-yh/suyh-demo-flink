package com.leomaster.utils.mq.sink;

import com.leomaster.utils.mq.DataRichSinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class MqDataSinkFunction extends DataRichSinkFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(MqDataSinkFunction.class);

    private final String routingKey;

    public MqDataSinkFunction(RMQConnectionConfig rmqConnectionConfig, String exchange, String routingKey){
        super(rmqConnectionConfig, exchange);
        this.routingKey = routingKey;
    }

    @Override
    public void invoke(String value, Context context){
        try{
            LOG.debug("sink to exchange: {}, routing key: {}, value: {}", super.exchange, routingKey, value);
            channel.basicPublish(super.exchange, routingKey, null,value.getBytes(StandardCharsets.UTF_8));
        }catch (Exception e){
            LOG.error("[Exception]",e);
        }
    }
}
