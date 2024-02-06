







前提，目前我这边是使用standalone 模式进行提交作业以及运行flink 集群

一直想调整flink 的日志配置，但发现flink 的日志配置最终都是通过log.file 这个命令行参数来获取的，并且最终得到的是一个绝对路径的文件。

显然，我已经跟踪到该值是在flink 的相关脚本里面进行解析并最终传入到java 命令行参数的。



```txt
首先，通过bin/flink 脚本生成了log.file 命令行参数，并传入了相关参数
```

> 将最后的启动命令解析拆解出来大概如下：
>
> 这里明确给定了log.file 的启动参数，但很明显这个启动参数是给flink 前台客户端的。

```shell
exec java 
-Dlog.file=/home/suyunhong/module/flink-1.18.0/log/flink-suyunhong-client-bi-dev.log 
-Dlog4j.configuration=file:/home/suyunhong/module/flink-1.18.0/conf/log4j-cli.properties 
-Dlog4j.configurationFile=file:/home/suyunhong/module/flink-1.18.0/conf/log4j-cli.properties 
-Dlogback.configurationFile=file:/home/suyunhong/module/flink-1.18.0/conf/logback.xml 
-classpath xxx.jar 
org.apache.flink.client.cli.CliFrontend run -d -p 4 -m localhost:8991 d01-debug-multi_job.jar
```







## taskmanager.sh

> 该脚本最终执行的一行命令是：

```shell
bin/flink-daemon.sh start taskexecutor 
--configDir /home/suyunhong/module/flink-1.18.0/conf 
-D taskmanager.memory.network.min=134217730b 
-D taskmanager.cpu.cores=8.0 
-D taskmanager.memory.task.off-heap.size=0b 
-D taskmanager.memory.jvm-metaspace.size=268435456b 
-D external-resources=none 
-D taskmanager.memory.jvm-overhead.min=201326592b 
-D taskmanager.memory.framework.off-heap.size=134217728b 
-D taskmanager.memory.network.max=134217730b 
-D taskmanager.memory.framework.heap.size=134217728b 
-D taskmanager.memory.managed.size=536870920b 
-D taskmanager.memory.task.heap.size=402653174b 
-D taskmanager.numberOfTaskSlots=8 
-D taskmanager.memory.jvm-overhead.max=201326592b
```



### flink-daemon.sh

> 关键一行命令

```shell
bin/java 
-Xmx536870902 -Xms536870902 -XX:MaxDirectMemorySize=268435458 -XX:MaxMetaspaceSize=268435456 -XX:+IgnoreUnrecognizedVMOptions 
-Dlog.file=/home/suyunhong/module/flink-1.18.0/log/flink-suyunhong-taskexecutor-2-bi-dev.log 
-Dlog4j.configuration=file:/home/suyunhong/module/flink-1.18.0/conf/log4j.properties 
-Dlog4j.configurationFile=file:/home/suyunhong/module/flink-1.18.0/conf/log4j.properties 
-Dlogback.configurationFile=file:/home/suyunhong/module/flink-1.18.0/conf/logback.xml 
-classpath  xxx.jar:xxx.jar:::
org.apache.flink.runtime.taskexecutor.TaskManagerRunner 
--configDir /home/suyunhong/module/flink-1.18.0/conf 
-D taskmanager.memory.network.min=134217730b 
-D taskmanager.cpu.cores=8.0 
-D taskmanager.memory.task.off-heap.size=0b 
-D taskmanager.memory.jvm-metaspace.size=268435456b 
-D external-resources=none 
-D taskmanager.memory.jvm-overhead.min=201326592b 
-D taskmanager.memory.framework.off-heap.size=134217728b 
-D taskmanager.memory.network.max=134217730b 
-D taskmanager.memory.framework.heap.size=134217728b 
-D taskmanager.memory.managed.size=536870920b 
-D taskmanager.memory.task.heap.size=402653174b 
-D taskmanager.numberOfTaskSlots=8 
-D taskmanager.memory.jvm-overhead.max=201326592b
```

> 所以这里的-Dlog.file 就是最终的日志文件。由脚本生成，关键位置在
>
> flink-daemon.sh 中的如下三行

```shell
FLINK_LOG_PREFIX="${FLINK_LOG_DIR}/flink-${FLINK_IDENT_STRING}-${DAEMON}-${id}-${HOSTNAME}"
log="${FLINK_LOG_PREFIX}.log"
out="${FLINK_LOG_PREFIX}.out"
```

> 做成这样，那么在log4j.properties 就不好处理日志文件了。



## 补充

```txt
按上面的配置，会在日志目录下面多出很多rockdb 的日志文件，所以需要将这些日志文件调整到别处。
```

### 补充配置

```yaml
# 将rockdb 的相关日志转到别的目录
state.backend.rocksdb.log.dir: /home/suyunhong/temp
```







## 结论

> 以flink 1.18.0 版本为例，只需要修改` bin/flink-daemon.sh` 以及`conf/log4j.properties`





