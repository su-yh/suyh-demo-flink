package com.suyh.d02.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class EnvDemo {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8082");

        StreamExecutionEnvironment env = StreamExecutionEnvironment
//                .getExecutionEnvironment();  // 自动识别是 远程集群 ，还是idea本地环境
                .getExecutionEnvironment(conf); // conf对象可以去修改一些参数

//                .createLocalEnvironment()
//        .createRemoteEnvironment("hadoop102", 8081,"/xxx")

        // 流跟批的结果现象，表现在输出结果中。如果是批处理，那么效果就是所有的数据一起处理完，而按流处理就是一条一条的输出效果。
        // 也就是说按流处理就会出现， (hello, 1) (hello, 2) 这样的输出会出现两次，每次+1。而批的话就一下得出最终结果 (hello, 2)
        // 流批一体：代码api是同一套，可以指定为 批，也可以指定为 流
        // 默认 STREAMING
        // 一般不在代码写死，提交时 参数指定：-Dexecution.runtime-mode=BATCH
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);


        env
//                .socketTextStream("hadoop102", 7777)
                .readTextFile("demo-01/input/word.txt")
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        env.execute();
        /** TODO 关于execute总结(了解)
         *     1、默认 env.execute()触发一个flink job：
         *          一个main方法可以调用多个execute，但是没意义，指定到第一个就会阻塞住
         *     2、env.executeAsync()，异步触发，不阻塞
         *         => 一个main方法里 executeAsync()个数 = 生成的flink job数
         *     3、思考：
         *         yarn-application 集群，提交一次，集群里会有几个flink job？
         *         =》 取决于 调用了n个 executeAsync()
         *         =》 对应 application集群里，会有n个job
         *         =》 对应 Jobmanager当中，会有 n个 JobMaster
         */
//        env.executeAsync();
        // ……
//        env.executeAsync();


    }
}
