package com.suyh;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author suyh
 * @since 2024-02-06
 */
public class DemoBatchApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSource<Long> longDataSource = env.generateSequence(1, 30);
        DistinctOperator<Long> distinct = longDataSource.distinct(new KeySelector<Long, Long>() {
            @Override
            public Long getKey(Long value) throws Exception {
                return value;
            }
        });

        MapOperator<Long, Long> map = distinct.map(new RichMapFunction<Long, Long>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("map open");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("map close");
            }

            @Override
            public Long map(Long value) throws Exception {
                System.out.println("map: " + value);
                TimeUnit.SECONDS.sleep(1);
                return value;
            }
        });

        FilterOperator<Long> filter = map.filter(new RichFilterFunction<Long>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("filter open");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("filter close");
            }

            @Override
            public boolean filter(Long value) throws Exception {
                System.out.println("filter: " + value);
//                TimeUnit.SECONDS.sleep(1);
                return true;
            }
        });

        filter.flatMap(new RichFlatMapFunction<Long, Long>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("flatmap open");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("floatmap close");
            }

            @Override
            public void flatMap(Long value, Collector<Long> out) throws Exception {
                System.out.println("flatMap: " + value);
                TimeUnit.SECONDS.sleep(1);
                out.collect(value);
            }
        }).printOnTaskManager("suyh");


        env.execute();
    }
}
