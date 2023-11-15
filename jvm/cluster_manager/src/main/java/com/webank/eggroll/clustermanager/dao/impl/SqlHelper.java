//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.enums.SqlMethod;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import com.baomidou.mybatisplus.core.toolkit.Assert;
import com.baomidou.mybatisplus.core.toolkit.CollectionUtils;

import java.util.List;
import java.util.function.Supplier;

import org.apache.ibatis.logging.Log;
import org.apache.ibatis.session.SqlSessionFactory;


public final class SqlHelper {
    public static SqlSessionFactory FACTORY;

    public SqlHelper() {
    }

    public static TableInfo table(Class<?> clazz) {
        TableInfo tableInfo = TableInfoHelper.getTableInfo(clazz);
        Assert.notNull(tableInfo, "Error: Cannot execute table Method, ClassGenericType not found.", new Object[0]);
        return tableInfo;
    }

    public static boolean retBool(Integer result) {
        return null != result && result >= 1;
    }

    public static boolean retBool(Long result) {
        return null != result && result >= 1L;
    }

    public static long retCount(Long result) {
        return null == result ? 0L : result;
    }

    public static <E> E getObject(Log log, List<E> list) {
        return getObject(() -> {
            return log;
        }, list);
    }

    public static <E> E getObject(Supplier<Log> supplier, List<E> list) {
        if (CollectionUtils.isNotEmpty(list)) {
            int size = list.size();
            if (size > 1) {
                Log log = (Log) supplier.get();
                log.warn(String.format("Warn: execute Method There are  %s results.", size));
            }

            return list.get(0);
        } else {
            return null;
        }
    }


    public static String getSqlStatement(Class<?> mapper, SqlMethod sqlMethod) {
        return mapper.getName() + "." + sqlMethod.getMethod();
    }

}
