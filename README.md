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

- 水位线(`watermark`)

  ```txt
  每一个数据的事件时间，又被称为数据的逻辑时间，跟随每一个数据产生。
  ```

  > 周期性生成水位线
  >
  > 主要数据就是一个时间戳

  1. 水位线生成策略(`assignTimestampsAndWatermarks`)

  2. watermark 时间

     ```txt
     watermark 时间决定窗口何时关闭，也就是等待迟到的数据。
     所以watermark 的时钟往往会需要 -1 ms
     ```

  3. 事件时间

     ```txt
     事件时间(逻辑时钟)决定该事件会被放在哪一个窗口
     ```

  4. `forBoundedOutOfOrderness(Duration.ofSeconds(5))`

     ```txt
     这种方式是直接将事件时间调慢5 秒钟，但是这种情况会直接影响flink 的计算延迟5 秒。
     所以这里的延迟时间一般都设置得比较小，一般在毫秒级别。
     ```

  5. `allowedLateness(Time.hours(2))`

     ```txt
     这个才是一般情况使用的处理迟到数据的方法。
     它不会影响事件时间，但是会让窗口延迟关闭，以将迟到的数据也最终处理完全。
     比起修改watermark ，使用处理迟到数据更合适。它不会影响事件时间，但是会保留处理迟到数据的能力。
     ```

  6. 其他

- 其他







## 问题

- `springboot` 如何集成`flink`

  ```txt
  主要是正常情况我们都使用spring-boot-web 组件，如果不使用web 组件，那么spring boot 启动之后很快就会程序退出。
  针对这个问题，我们要如何阻塞spring boot 应用，让其阻塞住，而不退出程序呢？
  ```

- 一条数据进入flink ，在经过窗口之后，一直在触发后面的流程

  > 场景

  ```txt
  事件时间窗口，添加一个flink 提供的处理时间触发器(`ContinuousProcessingTimeTrigger`)，该触发器一直触发，形成死循环。
  在一个较久远之前数据到来之后就没 有新的数据到来了，那么这个事件时间将永远不再更新。
  ```

  > 问题原因

  ```txt
  1. 在一个很久之前的数据到之后，不再有新的数据到来，导致事件时间将会固定不会更新，也就是说事件时间将不会向前打进。
  2. flink 提供的处理时间触发器(`ContinuousProcessingTimeTrigger`) 的触发条件逻辑，是取参数时间戳与处理时间戳对比，并取较小的那个值作为下一次触发器的时间戳。
  3. 参数传入的时间戳在这里取的就是事件时间戳，而该时间戳比处理时间要小很多且该值也固定不会再更新，所以下一次的触发时间一直都是很久之前，那么该触发器将一直触发。
  ```

  > 解决方案：自定义实现触发器
  >
  > 分析一下当前场景，其实需求比较简单：就是要每隔一段时间触发一次计算，且不管是否有新数据到来。

  ```txt
  这里就是不要管参数上面的时间戳，总是取处理时间戳即可。
  
  ```

  ```java
  import org.apache.flink.streaming.api.windowing.time.Time;
  import org.apache.flink.streaming.api.windowing.triggers.Trigger;
  import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
  import org.apache.flink.streaming.api.windowing.windows.Window;
  
  public class SuyhContinuousProcessingTimeTrigger<W extends Window> extends Trigger<Object, W> {
      private static final long serialVersionUID = 1L;
  
      private final long interval;
      private boolean init = false;
      private Long timerTimestamp = null;
  
      private SuyhContinuousProcessingTimeTrigger(long interval) {
          this.interval = interval;
      }
  
      @Override
      public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx)
              throws Exception {
          // 虽然这里的触发器逻辑跟元素没有任何关系，但是这个触发器没有提供初始化方法，所以也就只能在这里进行初始化操作了。
          if (!init) {
              init = true;
              registerNextFireTimestamp(ctx);
          }
          return TriggerResult.CONTINUE;
      }
  
      @Override
      public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
          return TriggerResult.CONTINUE;
      }
  
      /**
       * 注册的定时器到了的实现逻辑
       * <p>
       * 1. 注册下一个定时器
       * <p>
       * 2. 触发当前窗口计算
       */
      @Override
      public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx)
              throws Exception {
          // 注册的定时器到了的实现逻辑
          // 注册下一个定时器
          registerNextFireTimestamp(ctx);
  
          // 触发当前计算
          return TriggerResult.FIRE;
      }
  
      @Override
      public void clear(W window, TriggerContext ctx) throws Exception {
          if (timerTimestamp == null) {
              return;
          }
  
          // 清理定时器
          ctx.deleteProcessingTimeTimer(timerTimestamp);
          timerTimestamp = null;
      }
  
      public static <W extends Window> SuyhContinuousProcessingTimeTrigger<W> of(Time interval) {
          return new SuyhContinuousProcessingTimeTrigger<>(interval.toMilliseconds());
      }
  
      private void registerNextFireTimestamp(TriggerContext ctx) {
          timerTimestamp = ctx.getCurrentProcessingTime() + interval;
          ctx.registerProcessingTimeTimer(timerTimestamp);
      }
  }
  ```

- 其他