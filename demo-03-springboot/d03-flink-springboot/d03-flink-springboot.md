


## 自定义扩展配置项

1. 自定义扩展配置项

   > 该配置文件用于替代原配置文件：`flink-job.properties`
   >
   > 首先在flink 的配置文件`conf/flink-conf.yaml` 中添加自定义的配置文件所在位置。
   >
   > 参考如下：

   ```yaml
   flink.spring.yaml-path: /home/suyunhong/module/flink-1.18.0/job-conf.yaml
   ```

2. 打包

   > 使用mvn clean package 打包后，会将所有的包都打包拷贝到  build-dir 目录，这是通过插件maven-dependency-plugin 配置的

3. 依赖jars

   > job-2nd 下面生成的jar 包直接放到 usrlib/job-2nd/ 目录
   >
   > job-3rd 通过springboot 打包出来的jar，解压出来，把lib/ 目录下所有的jar 放到 usrlib/job3rd/ 目录
   >
   > flink-plus 通过springboot 打包出来的jar，解压出来，把lib/ 目录下所有的jar 放到 flink-plus/ 目录

4. 将提供的相关jar 包文件拷贝到lib 目录

   > 拷贝相关依赖包到 lib 目录

   ```shell
   # FLINK_HOME 是flink 的根目录，比如：/opt/module/flink-1.18.0
   # flink-plus 和usrlib 目录中的文件为当前我们所依赖的相关的 二方件、三方件以及flink 自带jar 以外的包文件
   cp -r flink-plus/ ${FLINK_HOME}/lib/
   cp -r usrlib/ ${FLINK_HOME}/lib/
   ```

5. 源代码

   > 对于flink-job 模块中的所有的都是业务源代码，只是按不同的模块进行区分，最终他们都会被插件maven-shade-plugin 打包到一起，不需要区分不同的包。
   >
   > flink-job-core 中的所有class 都会被打包到flink-job-app 中，所以flink-job-core 这个jar 就可以不用管了。

6. 其他



