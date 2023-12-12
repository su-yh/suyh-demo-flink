## suyh-demo-flink
flink 学习demo





## flink 的一些关键术语

- 并行度

- slot 槽

- 分区、分组

- 分流

- 合流

- `checkpoint`

- `Watermark` 水位线、水印

- 状态(有状态/无状态)

  ```txt
  状态就是指存起来的数据
  ```

  状态的分类：托管状态和原始状态

  ```txt
  托管状态(Managed State)：由flink 统一管理
  	算子状态(Operator State)
  	按键分区状态(Keyed State)
  原始状态：自己管理
  ```

- 其他







## 问题

- `springboot` 如何集成`flink`

  ```txt
  主要是正常情况我们都使用spring-boot-web 组件，如果不使用web 组件，那么spring boot 启动之后很快就会程序退出。
  针对这个问题，我们要如何阻塞spring boot 应用，让其阻塞住，而不退出程序呢？
  ```

- 其他