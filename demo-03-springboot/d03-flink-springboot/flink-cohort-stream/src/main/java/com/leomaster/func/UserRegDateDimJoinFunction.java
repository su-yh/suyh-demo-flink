package com.leomaster.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Desc:  用户注册时间维度关联接口
 */
public interface UserRegDateDimJoinFunction<T> {

    /**
     * 返回查询条件二元组；
     * 因为这里的规则比较特殊，所以参数暂时就不做抽象了
     * f0=uid；f1=channel；
     *
     * @param
     * @return
     */
    Tuple3<String, String,String> getCondition(T obj);

    /**
     * join维度关联的结果
     *
     * @param obj
     * @param userRegTimeResult
     * @throws Exception
     */
    void join(T obj, JSONObject userRegTimeResult) throws Exception;
}
