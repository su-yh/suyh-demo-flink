





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

## flink部署

### 集群角色

- JobManager
- TaskManger
- FlinkClient

### 下载安装包

官网：https://flink.apache.org

下载地址：https://dlcdn.apache.org/flink/flink-1.17.2/flink-1.17.2-bin-scala_2.12.tgz



 

```shell
# 解压
tar -zxvf flink-1.17.2-bin-scala_2.12.tgz -C /opt/module/
cd /opt/module/flink-1.17.2

```



### 配置

#### 修改配置 JobManager

指定一个服务实例为JobManager，然后进入到该主机，修改对应的配置文件

> vim conf/flink-conf.yaml

```properties
# JobManager节点地址.
jobmanager.rpc.address: flink01.isuyh.com
jobmanager.bind-host: 0.0.0.0
rest.address: flink01.isuyh.com
rest.bind-address: 0.0.0.0
# TaskManager节点地址.需要配置为当前机器名
taskmanager.bind-host: 0.0.0.0
taskmanager.host: flink01.isuyh.com

```

> 修改workers
>
> vim conf/workers

```properties
flink01.isuyh.com
flink02.isuyh.com
flink03.isuyh.com
```

> vim conf/masters

```properties
flink01.isuyh.com:8081
```

#### 修改配置TaskManager

将上面JobManager的配置对应修改

> vim conf/flink-conf.yaml

```properties
# JobManager节点地址.
jobmanager.rpc.address: flink01.isuyh.com
jobmanager.bind-host: 0.0.0.0
rest.address: flink01.isuyh.com
rest.bind-address: 0.0.0.0
# TaskManager节点地址.需要配置为当前机器名
taskmanager.bind-host: 0.0.0.0
# 主要是这里，自己的服务器域名
taskmanager.host: flink02.isuyh.com
```

另一个TaskManager

```properties
# JobManager节点地址.
jobmanager.rpc.address: flink01.isuyh.com
jobmanager.bind-host: 0.0.0.0
rest.address: flink01.isuyh.com
rest.bind-address: 0.0.0.0
# TaskManager节点地址.需要配置为当前机器名
taskmanager.bind-host: 0.0.0.0
# 主要是这里，自己的服务器域名
taskmanager.host: flink03.isuyh.com
```

### 启动/停止集群

> bin/start-cluster.sh
>
> bin/stop-cluster.sh
>
> 如果报错，就安装一个jdk 就好

### 网页访问

> http://flink01.isuyh.com:8081
>
> 而访问flink02.isuyh.com:8081 是访问不通的，看来是只能在JobManager 所在的ip 才能访问。

### 命令行提交job

```shell
bin/flink run -m hadoop102:8081 -c com.atguigu.wc.SocketStreamWordCount ./FlinkTutorial-1.0-SNAPSHOT.jar
```

### 部署模式

> 它们的区别主要在于：集群的生命周期以及资源 的分配方式；以及应用的main 方法到底在哪里执行--客户端(Client) 还是JobManager

- 会话模式(Session Mode)

  ```txt
  会话模式比较适合于单个规模小、执行时间短的大量作业。
  ```

  

- 单作业模式(Per-Job Mode)  `在新版本中已经被标记为过时了`

  ```txt
  一个作业一个flink集群，它需要借助一些外部资源 管理系统，比较yarn 
  ```

  

- 应用模式(Application Mode)

  ```txt
  直接把应用提交到JobManager 上运行。而这也就代表着，我们需要为每一个提交的应用单独启动一个JobManager，也就是创建一个集群。这个JobManager 只为执行这一个应用而存在，执行结束之后JobManager 也就关闭了，这就是所谓的应用模式。
  ```

  

### 运行模式

> 运行模式主要在于由谁来管理资源

- Standalone (了解)
- YARN(重点)
- K8S(了解)



