package com.suyh.d02;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * TODO: suyh - 解析hdfs 上的文件
 * @author suyh
 * @since 2024-01-13
 */
public class ReadYAMLFromHDFS {
    public static void main(String[] args) throws Exception {
        // 创建 ExecutionEnvironment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 指定要读取的 HDFS 文件路径
        String hdfsFilePath = "hdfs://your-hdfs-path/your-file.yaml";
        TextInputFormat textInputFormat = new TextInputFormat(new Path(hdfsFilePath));

        // 从 HDFS 中读取 YAML 文件
        env
                .readFile(new TextInputFormat(new Path(hdfsFilePath)), hdfsFilePath)
                .flatMap(new YamlParser())
                .print();

        // 执行作业
        env.execute("Read YAML from HDFS");
    }

    // 自定义 FlatMapFunction 用于解析 YAML 内容
    public static final class YamlParser implements FlatMapFunction<String, Map<String, Object>> {
        @Override
        public void flatMap(String value, Collector<Map<String, Object>> out) {
            // 使用 SnakeYAML 解析 YAML
            Yaml yaml = new Yaml();
            try (InputStream inputStream = IOUtils.toInputStream(value, StandardCharsets.UTF_8)) {
                Map<String, Object> yamlMap = yaml.load(inputStream);
                out.collect(yamlMap);
            } catch (IOException e) {
                // 处理异常
                e.printStackTrace();
            }
        }
    }
}

