//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.enums.SqlMethod;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import com.baomidou.mybatisplus.core.toolkit.Assert;
import com.baomidou.mybatisplus.core.toolkit.ClassUtils;
import com.baomidou.mybatisplus.core.toolkit.CollectionUtils;
import com.baomidou.mybatisplus.core.toolkit.ExceptionUtils;
import com.baomidou.mybatisplus.core.toolkit.GlobalConfigUtils;
import com.baomidou.mybatisplus.core.toolkit.support.SFunction;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.reflection.ExceptionUtil;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
//import org.mybatis.spring.MyBatisExceptionTranslator;
//import org.mybatis.spring.SqlSessionHolder;
//import org.mybatis.spring.SqlSessionUtils;


public final class SqlHelper {
    public static SqlSessionFactory FACTORY;

    public SqlHelper() {
    }

//    public static SqlSession sqlSessionBatch(Class<?> clazz) {
//        return sqlSessionFactory(clazz).openSession(ExecutorType.BATCH);
//    }

    /**
     * @deprecated
     */
//    @Deprecated
//    public static SqlSessionFactory sqlSessionFactory(Class<?> clazz) {
//        return GlobalConfigUtils.currentSessionFactory(clazz);
//    }

//    /** @deprecated */
//    @Deprecated
//    public static SqlSession sqlSession(Class<?> clazz) {
//        return SqlSessionUtils.getSqlSession(GlobalConfigUtils.currentSessionFactory(clazz));
//    }
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

    /** @deprecated */
//    @Deprecated
//    public static boolean executeBatch(Class<?> entityClass, Log log, Consumer<SqlSession> consumer) {
//        return executeBatch(sqlSessionFactory(entityClass), log, consumer);
//    }

//    public static boolean executeBatch(SqlSessionFactory sqlSessionFactory, Log log, Consumer<SqlSession> consumer) {
//        try {
//            SqlSessionHolder sqlSessionHolder = (SqlSessionHolder)TransactionSynchronizationManager.getResource(sqlSessionFactory);
//            boolean transaction = TransactionSynchronizationManager.isSynchronizationActive();
//            SqlSession sqlSession;
//            if (sqlSessionHolder != null) {
//                sqlSession = sqlSessionHolder.getSqlSession();
//                sqlSession.commit(!transaction);
//            }
//
//            sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH);
//            if (!transaction) {
//                log.warn("SqlSession [" + sqlSession + "] Transaction not enabled");
//            }
//
//            boolean var6;
//            try {
//                consumer.accept(sqlSession);
//                sqlSession.commit(!transaction);
//                var6 = true;
//            } catch (Throwable var14) {
//                sqlSession.rollback();
//                Throwable unwrapped = ExceptionUtil.unwrapThrowable(var14);
//                if (unwrapped instanceof PersistenceException) {
//                    MyBatisExceptionTranslator myBatisExceptionTranslator = new MyBatisExceptionTranslator(sqlSessionFactory.getConfiguration().getEnvironment().getDataSource(), true);
//                    Throwable throwable = myBatisExceptionTranslator.translateExceptionIfPossible((PersistenceException)unwrapped);
//                    if (throwable != null) {
//                        throw throwable;
//                    }
//                }
//
//                throw ExceptionUtils.mpe(unwrapped);
//            } finally {
//                sqlSession.close();
//            }
//
//            return var6;
//        } catch (Throwable var16) {
//            throw var16;
//        }
//    }

    /**
     * @deprecated
     */
//    @Deprecated
//    public static <E> boolean executeBatch(Class<?> entityClass, Log log, Collection<E> list, int batchSize, BiConsumer<SqlSession, E> consumer) {
//        return executeBatch(sqlSessionFactory(entityClass), log, list, batchSize, consumer);
//    }

//    public static <E> boolean executeBatch(SqlSessionFactory sqlSessionFactory, Log log, Collection<E> list, int batchSize, BiConsumer<SqlSession, E> consumer) {
//        Assert.isFalse(batchSize < 1, "batchSize must not be less than one", new Object[0]);
//        return !CollectionUtils.isEmpty(list) && executeBatch(sqlSessionFactory, log, (sqlSession) -> {
//            int size = list.size();
//            int idxLimit = Math.min(batchSize, size);
//            int i = 1;
//
//            for(Iterator var7 = list.iterator(); var7.hasNext(); ++i) {
//                E element = var7.next();
//                consumer.accept(sqlSession, element);
//                if (i == idxLimit) {
//                    sqlSession.flushStatements();
//                    idxLimit = Math.min(idxLimit + batchSize, size);
//                }
//            }
//
//        });
//    }

//    public static <E> boolean saveOrUpdateBatch(Class<?> entityClass, Class<?> mapper, Log log, Collection<E> list, int batchSize, BiPredicate<SqlSession, E> predicate, BiConsumer<SqlSession, E> consumer) {
//        String sqlStatement = getSqlStatement(mapper, SqlMethod.INSERT_ONE);
//        return executeBatch(entityClass, log, list, batchSize, (sqlSession, entity) -> {
//            if (predicate.test(sqlSession, entity)) {
//                sqlSession.insert(sqlStatement, entity);
//            } else {
//                consumer.accept(sqlSession, entity);
//            }
//
//        });
//    }
    public static String getSqlStatement(Class<?> mapper, SqlMethod sqlMethod) {
        return mapper.getName() + "." + sqlMethod.getMethod();
    }

//    public static <T, M extends BaseMapper<T>> M getMapper(Class<T> entityClass, SqlSession sqlSession) {
//        Assert.notNull(entityClass, "entityClass can't be null!", new Object[0]);
//        TableInfo tableInfo = (TableInfo)Optional.ofNullable(TableInfoHelper.getTableInfo(entityClass)).orElseThrow(() -> {
//            return ExceptionUtils.mpe("Can not find TableInfo from Class: \"%s\".", new Object[]{entityClass.getName()});
//        });
//        Class<?> mapperClass = ClassUtils.toClassConfident(tableInfo.getCurrentNamespace());
//        return (BaseMapper)tableInfo.getConfiguration().getMapper(mapperClass, sqlSession);
//    }

//    public static <T, R, M extends BaseMapper<T>> R execute(Class<T> entityClass, SFunction<M, R> sFunction) {
//        SqlSession sqlSession = sqlSession(entityClass);
//
//        Object var3;
//        try {
//            var3 = sFunction.apply(getMapper(entityClass, sqlSession));
//        } finally {
//            SqlSessionUtils.closeSqlSession(sqlSession, GlobalConfigUtils.currentSessionFactory(entityClass));
//        }
//
//        return var3;
//    }
}
