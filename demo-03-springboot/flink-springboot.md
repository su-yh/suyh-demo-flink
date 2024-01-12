



> 在flink 中集成spring boot 依赖，并使用spring boot 提供的相关功能



### 注意点

1. JobManager 和TaskManager 各自是独立的进程，他们可能在同一台机器实现上运行，也可能分散在不同的实例上运行。
2. 在同一个进程中，运行多个 spring AppliicationContext 会导致日志报错，具体的根本原因还未找到。不过正常情况下，我们也不应该在同一个进程里面运行两个spring 容器，所以基本可以忽略这个问题。
3. 日志配置文件，我们应该使用flink 提供的日志配置文件来集成spring boot，所以在打包的时候我们需要把对应的日志文件排除掉，而不打包在最终的jar 中。
4. flink 的类加载机制有一些特别处理，同时spring boot 的类加载机制也有一些特别处理，更重要的是spring boot 的打包机制也会有一些特点，主要是spring boot 的SPI 机制。所以我们需要把spring boot 的依赖包使用spring boot 的方式进行打包，最后提交到flink 的lib 目录下，让flink 在运行的时候使用AppClassLoader 来加载这些lib。
5. 还有一点，spring boot 的lib 在某些依赖被排除之后，需要将对应的目录下面的lib 清空并重新上传spring boot 打的包





## yarn 模式

```txt
如果是yarn 模式，则可以将这些依赖的jar 包提前上传到hdfs ，然后在提交job 的时候指定hdfs 的相应路径
```

1. 首先使用spring-boot-maven-plugin 插件打包，然后解压出文件找到lib 目录，将该lib 目录上传到 hdfs://














