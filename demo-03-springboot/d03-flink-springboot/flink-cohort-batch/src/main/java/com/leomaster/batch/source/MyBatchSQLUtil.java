package com.leomaster.batch.source;

import com.google.common.base.CaseFormat;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.compress.utils.Lists;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

/**
 * Desc: 从MySQL数据中查询数据的工具类
 * 完成ORM，对象关系映射
 * O：Object对象       Java中对象
 * R：Relation关系     关系型数据库
 * M:Mapping映射      将Java中的对象和关系型数据库的表中的记录建立起映射关系
 * 数据库                 Java
 * 表t_student           类Student
 * 字段id，name           属性id，name
 * 记录 100，zs           对象100，zs
 * ResultSet(一条条记录)             List(一个个Java对象)
 */
@Slf4j
public class MyBatchSQLUtil {

    /**
     * @param sql               执行的查询语句
     * @param clz               返回的数据类型
     * @param underScoreToCamel 是否将下划线转换为驼峰命名法
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel, HikariDataSource hikariDataSource) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = hikariDataSource.getConnection();

            //创建数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            // 100      zs      20
            // 200		ls 		30
            rs = ps.executeQuery();
            //处理结果集
            //查询结果的元数据信息
            // id		student_name	age
            ResultSetMetaData metaData = rs.getMetaData();
            List<T> resultList = new ArrayList<T>();
            //判断结果集中是否存在数据，如果有，那么进行一次循环
            while (rs.next()) {
                //创建一个对象，用于封装查询出来一条结果集中的数据
                T obj = clz.newInstance();
                //对查询的所有列进行遍历，获取每一列的名称
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    String propertyName = columnName;
                    if (underScoreToCamel) {
                        //如果指定将下划线转换为驼峰命名法的值为 true，通过guava工具类，将表中的列转换为类属性的驼峰命名法的形式
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //调用apache的commons-bean中工具类，给obj属性赋值
                    BeanUtils.setProperty(obj, propertyName, rs.getObject(i));
                }
                //将当前结果中的一行数据封装的obj对象放到list集合中
                resultList.add(obj);
            }

            return resultList;
        } catch (Exception e) {
            log.error("[从MySQL查询数据失败]sql:{}", sql, e);
        } finally {
            hikariDataSource.evictConnection(conn);
        }
        return Lists.newArrayList();
    }

    /**
     * @param sqls              执行的查询语句集合
     * @param clz               返回的数据类型
     * @param underScoreToCamel 是否将下划线转换为驼峰命名法
     * @param <T>
     * @return
     */
    public static <T> List<List<T>> queryLists(List<String> sqls, Class<T> clz, boolean underScoreToCamel, HikariDataSource hikariDataSource) {
        Connection conn = null;
        List<PreparedStatement> pss = new ArrayList<>(sqls.size());
        List<ResultSet> rss = new ArrayList<>(sqls.size());
        List<List<T>> allResults = new ArrayList<>(sqls.size());
        try {
            conn = hikariDataSource.getConnection();

            for (String sql : sqls) {
                //创建数据库操作对象
                PreparedStatement ps = conn.prepareStatement(sql);
                //执行SQL语句
                // 100      zs      20
                // 200		ls 		30
                ResultSet rs = ps.executeQuery();
                //处理结果集
                //查询结果的元数据信息
                // id		student_name	age
                ResultSetMetaData metaData = rs.getMetaData();
                List<T> resultList = new ArrayList<T>();
                //判断结果集中是否存在数据，如果有，那么进行一次循环
                while (rs.next()) {
                    //创建一个对象，用于封装查询出来一条结果集中的数据
                    T obj = clz.newInstance();
                    //对查询的所有列进行遍历，获取每一列的名称
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i);
                        String propertyName = columnName;
                        if (underScoreToCamel) {
                            //如果指定将下划线转换为驼峰命名法的值为 true，通过guava工具类，将表中的列转换为类属性的驼峰命名法的形式
                            propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                        }
                        //调用apache的commons-bean中工具类，给obj属性赋值
                        BeanUtils.setProperty(obj, propertyName, rs.getObject(i));
                    }
                    //将当前结果中的一行数据封装的obj对象放到list集合中
                    resultList.add(obj);
                }

                pss.add(ps);
                rss.add(rs);
                allResults.add(resultList);
            }

            return allResults;
        } catch (Exception e) {
            log.error("[从MySQL查询数据失败]", e);
        } finally {
            if (conn != null) {
                hikariDataSource.evictConnection(conn);
            }
        }
        return null;
    }
}
