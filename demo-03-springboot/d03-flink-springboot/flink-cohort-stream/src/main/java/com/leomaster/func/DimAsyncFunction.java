package com.leomaster.func;

import com.aiteer.springboot.core.context.FlinkSpringContext;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.leomaster.utils.MySQLUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Desc:  自定义维度异步查询的函数
 * 模板方法设计模式
 * 在父类中只定义方法的声明，让整个流程跑通
 * 具体的实现延迟到子类中实现
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DimAsyncFunction.class);

    //线程池对象的父接口生命（多态）
    private ExecutorService executorService;

    //维度的表名
    private final String tableName;

    private final Map<String, Object> configProperties;

    protected StringRedisTemplate redisTemplate;
    protected HikariDataSource hikariDataSource;

    public DimAsyncFunction(String tableName, Map<String, Object> configProperties) {
        this.tableName = tableName;
        this.configProperties = configProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        FlinkSpringContext.init(configProperties);

        this.hikariDataSource = FlinkSpringContext.getBean(HikariDataSource.class);
        this.redisTemplate = FlinkSpringContext.getBean(StringRedisTemplate.class);
        this.executorService = FlinkSpringContext.getBean("cohortThreadPool", ExecutorService.class);
    }

    @Override
    public void close() throws Exception {
        super.close();

        FlinkSpringContext.closeContext();
    }

    public void dimFailed(T obj) {

    }

    /**
     * 发送异步请求的方法
     *
     * @param obj          流中的事实数据
     * @param resultFuture 异步处理结束之后，返回结果
     * @throws Exception
     */
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            //发送异步请求
                            long start = System.currentTimeMillis();
                            //从流中事实数据获取key
                            Tuple2<String, String>[] whereClause = getCondition(obj);

                            //根据维度的主键到维度表中进行查询
                            JSONObject dimInfoJsonObj = getDimInfo(tableName, whereClause);
                            //System.out.println("维度数据Json格式：" + dimInfoJsonObj);

                            if (dimInfoJsonObj != null) {
                                //维度关联  流中的事实数据和查询出来的维度数据进行关联
                                join(obj, dimInfoJsonObj);
                            } else {
                                dimFailed(obj);
                            }
                            //System.out.println("维度关联后的对象:" + obj);
                            long end = System.currentTimeMillis();
                            LOGGER.debug("异步维度查询耗时{}毫秒", (end - start));
                            //将关联后的数据数据继续向下传递
                            resultFuture.complete(Collections.singleton(obj));
                        } catch (Exception e) {
                            LOGGER.error("维度异步查询失败, table name: {}", tableName, e);
                            throw new RuntimeException(tableName + "维度异步查询失败");
                        }
                    }
                }
        );
    }

    private JSONObject getDimInfo(String tableName, Tuple2<String, String>... cloNameAndValue) {
        //拼接查询条件
        String whereSql = " where ";
        String redisKey = "dim:" + tableName.toLowerCase() + ":";
        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> tuple2 = cloNameAndValue[i];
            String filedName = tuple2.f0;
            String fieldValue = tuple2.f1;
            if (i > 0) {
                whereSql += " and ";
                redisKey += "_";
            }
            whereSql += filedName + "='" + fieldValue + "'";
            redisKey += fieldValue;
        }

        //维度数据的json字符串形式
        String dimJsonStr = redisTemplate.opsForValue().get(redisKey);
        //维度数据的json对象形式
        JSONObject dimJsonObj = null;

        //判断是否从Redis中查询到了数据
        if (dimJsonStr != null) {
            if (dimJsonStr.isEmpty()) {
                return null;
            } else {
                dimJsonObj = JSON.parseObject(dimJsonStr);
                LOGGER.debug("查询维度，从redis获取到缓存 {}", dimJsonStr);
                return dimJsonObj;
            }
        } else {
            //如果在Redis中没有查到数据，需要到Phoenix中查询
            String sql = "select * from " + tableName + whereSql;
            LOGGER.debug("查询维度的SQL {}", sql);
            List<JSONObject> dimList = MySQLUtil.queryList(sql, JSONObject.class, true, hikariDataSource);

            //将查询出来的数据放到Redis中缓存起来
            //对于维度查询来讲，一般都是根据主键进行查询，不可能返回多条记录，只会有一条
            if (dimList != null && dimList.size() > 0) {
                dimJsonObj = dimList.get(0);
                redisTemplate.opsForValue().set(redisKey, dimJsonObj.toJSONString(), 1, TimeUnit.DAYS);
            } else {
                // 数据库中不存在值也缓存redis 一分钟，不要让所有查询都打到数据库中，减小数据库的压力。
                redisTemplate.opsForValue().set(redisKey, "", 1, TimeUnit.MINUTES);
            }
        }

        return dimJsonObj;
    }
}
