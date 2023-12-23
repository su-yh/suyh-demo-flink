package com.suyh.d05.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.suyh.d05.boot.DemoApplication;
import com.suyh.d05.boot.entity.UserEntity;
import com.suyh.d05.boot.mapper.UserMapper;
import com.suyh.d05.boot.runner.DemoRunner;
import com.suyh.d05.component.SuyhComponent;
import com.suyh.d05.util.JsonUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author suyh
 * @since 2023-12-23
 */
public class RichFlatMap extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
    private static final Logger log = LoggerFactory.getLogger(RichFlatMap.class);

    public RichFlatMap(String[] args) {
        this.args = args;
    }
    private final String[] args;
    private ConfigurableApplicationContext context;
    private DemoRunner demoRunner;
    private SuyhComponent suyhComponent;
    private UserMapper userMapper;
    private ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        context = SpringApplication.run(DemoApplication.class, args);
        demoRunner = context.getBean(DemoRunner.class);
        suyhComponent = context.getBean(SuyhComponent.class);
        userMapper = context.getBean(UserMapper.class);
        objectMapper = context.getBean(ObjectMapper.class);
        JsonUtils.initMapper(objectMapper);
    }

    @Override
    public void close() throws Exception {
        super.close();
        context.close();
    }

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        log.info("suyh - value: {}", value);
        System.out.println("value: " + value);
        demoRunner.showHello();
        suyhComponent.showHello();
        UserEntity userEntity = userMapper.selectById(1L);
        System.out.println("userEntity: " + userEntity);
        System.out.println("userEntity json: " + JsonUtils.serializable(userEntity));
        // value: 一行数据
        String[] words = value.split(" ");
        for (String word : words) {
            Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);

            // 调用采集器，使用Collector 向下游发送数据
            out.collect(wordTuple2);
        }
    }
}

