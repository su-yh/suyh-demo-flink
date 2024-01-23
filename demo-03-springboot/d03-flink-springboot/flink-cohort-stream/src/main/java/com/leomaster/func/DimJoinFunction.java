package com.leomaster.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Desc:  维度关联接口
 */
public interface DimJoinFunction<T> {

    //需要提供一个获取key的方法，但是这个方法如何实现不知道
    Tuple2<String, String>[] getCondition(T obj);

    //流中的事实数据和查询出来的维度数据进行关联
    void join(T obj, JSONObject dimInfoJsonObj) throws Exception;
}
