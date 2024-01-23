


## 自定义扩展配置项

3. 自定义扩展配置项

   > 该配置文件用于替代原配置文件：`flink-job.properties`
   >
   > 首先在flink 的配置文件`conf/flink-conf.yaml` 中添加自定义的配置文件所在位置。
   >
   > 参考如下：

   ```yaml
   flink.spring.yaml-path: /home/suyunhong/module/flink-1.18.0/job-conf.yaml
   ```

5. 将提供的相关jar 包文件拷贝到lib 目录

   > 拷贝相关依赖包到 lib 目录

   ```shell
   # FLINK_HOME 是flink 的根目录，比如：/opt/module/flink-1.18.0
   # flink-plus 和usrlib 目录中的文件为当前我们所依赖的相关的 二方件、三方件以及flink 自带jar 以外的包文件
   cp -r flink-plus/ ${FLINK_HOME}/lib/
   cp -r usrlib/ ${FLINK_HOME}/lib/
   ```