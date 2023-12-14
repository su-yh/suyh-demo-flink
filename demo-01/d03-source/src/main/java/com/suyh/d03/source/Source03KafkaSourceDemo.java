package com.suyh.d03.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO: suyh - 没有成功，报错了：Timed out waiting for a node assignment. Call: describeTopics
 *
 * @author cjp
 * @version 1.0
 */
public class Source03KafkaSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 需要进入到kafka 生产者模式
        // 进入到docker 容器：sudo docker exec -it q18qetlnsh2bnu_kafka-kafka-1 /bin/bash
        // 进入到kafka 控制台生产者客户端：kafka-console-producer.sh --broker-list localhost:9092 --topic topic_1

        // 从Kafka读： 新Source架构
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.8.34:9092") // 指定kafka节点的地址和端口
                .setGroupId("atguigu")  // 指定消费者组的id
                .setTopics("topic_1")   // 指定消费的 Topic
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 指定 反序列化器，这个是反序列化value
//                .setStartingOffsets(OffsetsInitializer.latest())  // flink消费kafka的策略
                .setStartingOffsets(OffsetsInitializer.earliest())  // flink消费kafka的策略
                .build();


        env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
//                .fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)), "kafkaSource")
                .print();


        env.execute();
    }
}
/**
 *   kafka消费者的参数：
 *      auto.reset.offsets
 *          earliest: 如果有offset，从offset继续消费; 如果没有offset，从 最早 消费
 *          latest  : 如果有offset，从offset继续消费; 如果没有offset，从 最新 消费
 *
 *   flink的kafkasource，offset消费策略：OffsetsInitializer，默认是 earliest
 *          earliest: 一定从 最早 消费
 *          latest  : 一定从 最新 消费
 *
 *
 *
 */