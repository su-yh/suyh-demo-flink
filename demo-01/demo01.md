





## provided 的本地运行

idea 环境

![image-20231212154645707](demo01.assets/image-20231212154645707.png)





## 分层api

- 有状态流处理

  process

- 核心APIS

  - DataStream API(推荐)
  - DataSet API(过时了)

- Table API

  声明式领域专用语言

- SQL

  最高层语言

## 核心概念

> 同一个算子，不同子任务必须分开。

### 并行度

```txt
每一个并行度被叫做一个子任务。
在Flink执行过程中，每一个算子（operator）可以包含一个或多个子任务（operator subtask），这些子任务在不同的线程、不同的物理机或不同的容器中完全独立地执行。
并行度是一种动态的概念，表示 实际 运行占用的slot 的数量
```

### 任务槽

```txt
slot 是一种静态的概念，表示 最大的并发上限
```

> 要求：slot 数量 >= job并行度(算子最大并行度)，job 才能运行



