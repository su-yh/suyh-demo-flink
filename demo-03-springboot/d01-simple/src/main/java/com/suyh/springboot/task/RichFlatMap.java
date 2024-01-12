package com.suyh.springboot.task;

import com.suyh.springboot.boot.taskmgr.TaskManagerSpringContext;
import com.suyh.springboot.boot.taskmgr.entity.FlinkUserEntity;
import com.suyh.springboot.boot.taskmgr.mapper.FlinkUserMapper;
import com.suyh.springboot.boot.taskmgr.runner.DemoRunner;
import com.suyh.springboot.component.SuyhComponent;
import com.suyh.springboot.util.JsonUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.UrlResource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

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
    private DemoRunner demoRunner;
    private SuyhComponent suyhComponent;
    private FlinkUserMapper flinkUserMapper;

    private static List<String> readCandidateConfigurations(URL url) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new UrlResource(url).getInputStream(), StandardCharsets.UTF_8))) {
            List<String> candidates = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                line = stripComment(line);
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                candidates.add(line);
            }
            return candidates;
        } catch (IOException ex) {
            throw new IllegalArgumentException("Unable to load configurations from location [" + url + "]", ex);
        }
    }

    private static final String COMMENT_START = "#";

    private static String stripComment(String line) {
        int commentStart = line.indexOf(COMMENT_START);
        if (commentStart == -1) {
            return line;
        }
        return line.substring(0, commentStart);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        {
            /*
             *
             * No qualifying bean of type 'org.springframework.boot.web.servlet.server.ServletWebServerFactory'
             * Unable to start AnnotationConfigServletWebServerApplicationContext due to missing ServletWebServerFactory bean
             *
             * org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory  implement ServletWebServerFactory
             *
             * org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryConfiguration.EmbeddedTomcat
             *
             * org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration
             *
             *
             * META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
             */
            List<String> importCandidates = new ArrayList<>();

            ClassLoader currentClassLoader = this.getClass().getClassLoader();
            Enumeration<URL> urls = currentClassLoader.getResources("META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports");

            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                System.out.println("suyh - url: " + url.toString());
                importCandidates.addAll(readCandidateConfigurations(url));
            }

            System.out.println("importCandidates size: " + importCandidates.size());
        }
//        {
//            List<String> importCandidates = new ArrayList<>();
//
//            Enumeration<URL> urls = classLoader.getResources("META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports");
//
//            while (urls.hasMoreElements()) {
//                URL url = urls.nextElement();
//                System.out.println("suyh - url: " + url.toString());
//                importCandidates.addAll(readCandidateConfigurations(url));
//            }
//
//            System.out.println("importCandidates size: " + importCandidates.size());
//        }
//
//        {
//            ClassLoader classLoader = this.getClass().getClassLoader();
//            ImportCandidates load = ImportCandidates.load(AutoConfiguration.class, classLoader);
//            List<String> configurations = new ArrayList<>();
//            load.forEach(configurations::add);
//            System.out.println("configurations size: " + configurations.size());
//            System.out.println("exists ServletWebServerFactoryAutoConfiguration: " + configurations.contains("org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration"));
//        }
//        {
//            ImportCandidates load = ImportCandidates.load(AutoConfiguration.class, null);
//            List<String> configurations = new ArrayList<>();
//            load.forEach(configurations::add);
//            System.out.println("[null] configurations size: " + configurations.size());
//            System.out.println("[null] exists ServletWebServerFactoryAutoConfiguration: " + configurations.contains("org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration"));
//        }

        ConfigurableApplicationContext context = TaskManagerSpringContext.getContext(args);
        demoRunner = context.getBean(DemoRunner.class);
        suyhComponent = context.getBean(SuyhComponent.class);
        flinkUserMapper = context.getBean(FlinkUserMapper.class);
    }

    @Override
    public void close() throws Exception {
        super.close();
        TaskManagerSpringContext.closeContext();
    }

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        log.info("suyh - value: {}", value);
        System.out.println("value: " + value);
        demoRunner.showHello();
        suyhComponent.showHello();
        FlinkUserEntity flinkUserEntity = flinkUserMapper.selectById(1L);
        System.out.println("userEntity: " + flinkUserEntity);
        System.out.println("userEntity json: " + JsonUtils.serializable(flinkUserEntity));
        // value: 一行数据
        String[] words = value.split(" ");
        for (String word : words) {
            Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);

            // 调用采集器，使用Collector 向下游发送数据
            out.collect(wordTuple2);
        }
    }
}

