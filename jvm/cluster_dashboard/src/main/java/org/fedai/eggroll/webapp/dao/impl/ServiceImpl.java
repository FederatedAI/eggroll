//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.fedai.eggroll.webapp.dao.impl;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.enums.SqlMethod;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import com.baomidou.mybatisplus.core.toolkit.Assert;
import com.baomidou.mybatisplus.core.toolkit.CollectionUtils;
import com.baomidou.mybatisplus.core.toolkit.ReflectionKit;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.google.inject.Inject;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.fedai.eggroll.clustermanager.dao.impl.SqlHelper;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;


public class ServiceImpl<M extends BaseMapper<T>, T> implements IService<T> {
    protected Log log = LogFactory.getLog(this.getClass());
    @Inject
    protected M baseMapper;
    protected Class<T> entityClass = this.currentModelClass();
    protected Class<M> mapperClass = this.currentMapperClass();

    public ServiceImpl() {
    }

    @Override
    public M getBaseMapper() {
        return this.baseMapper;
    }

    @Override
    public Class<T> getEntityClass() {
        return this.entityClass;
    }

    /**
     * @deprecated
     */
    @Deprecated
    protected boolean retBool(Integer result) {
        return SqlHelper.retBool(result);
    }

    protected Class<M> currentMapperClass() {
        return (Class<M>) ReflectionKit.getSuperClassGenericType(this.getClass(), ServiceImpl.class, 0);
    }

    protected Class<T> currentModelClass() {
        return (Class<T>) ReflectionKit.getSuperClassGenericType(this.getClass(), ServiceImpl.class, 1);
    }

    protected String getSqlStatement(SqlMethod sqlMethod) {
        return SqlHelper.getSqlStatement(this.mapperClass, sqlMethod);
    }


    @Override
    public boolean saveOrUpdate(T entity) {
        if (null == entity) {
            return false;
        } else {
            TableInfo tableInfo = TableInfoHelper.getTableInfo(this.entityClass);
            Assert.notNull(tableInfo, "error: can not execute. because can not find cache of TableInfo for entity!");
            String keyProperty = tableInfo.getKeyProperty();
            Assert.notEmpty(keyProperty, "error: can not execute. because can not find column for id from entity!");
            Object idVal = tableInfo.getPropertyValue(entity, tableInfo.getKeyProperty());
            return !StringUtils.checkValNull(idVal) && !Objects.isNull(this.getById((Serializable) idVal)) ? this.updateById(entity) : this.save(entity);
        }
    }

    @Override
    public T getOne(Wrapper<T> queryWrapper, boolean throwEx) {
        return throwEx ? this.baseMapper.selectOne(queryWrapper) : SqlHelper.getObject(this.log, this.baseMapper.selectList(queryWrapper));
    }

    @Override
    public Optional<T> getOneOpt(Wrapper<T> queryWrapper, boolean throwEx) {
        return Optional.empty();
    }

    @Override
    public Map<String, Object> getMap(Wrapper<T> queryWrapper) {
        return SqlHelper.getObject(this.log, this.baseMapper.selectMaps(queryWrapper));
    }

    @Override
    public <V> V getObj(Wrapper<T> queryWrapper, Function<? super Object, V> mapper) {
        return SqlHelper.getObject(this.log, this.listObjs(queryWrapper, mapper));
    }

    @Override
    public boolean saveBatch(Collection<T> entityList, int batchSize) {
        return false;
    }

    @Override
    public boolean saveOrUpdateBatch(Collection<T> entityList, int batchSize) {
        return false;
    }

    @Override
    public boolean removeById(Serializable id) {
        TableInfo tableInfo = TableInfoHelper.getTableInfo(this.getEntityClass());
        return tableInfo.isWithLogicDelete() && tableInfo.isWithUpdateFill() ? this.removeById(id, true) : SqlHelper.retBool(this.getBaseMapper().deleteById(id));
    }

    @Override
    public boolean removeByIds(Collection<?> list) {
        if (CollectionUtils.isEmpty(list)) {
            return false;
        } else {
            TableInfo tableInfo = TableInfoHelper.getTableInfo(this.getEntityClass());
            return tableInfo.isWithLogicDelete() && tableInfo.isWithUpdateFill() ? this.removeBatchByIds(list, true) : SqlHelper.retBool(this.getBaseMapper().deleteBatchIds(list));
        }
    }

    @Override
    public boolean removeById(Serializable id, boolean useFill) {
        TableInfo tableInfo = TableInfoHelper.getTableInfo(this.entityClass);
        if (useFill && tableInfo.isWithLogicDelete() && !this.entityClass.isAssignableFrom(id.getClass())) {
            T instance = tableInfo.newInstance();
            tableInfo.setPropertyValue(instance, tableInfo.getKeyProperty(), id);
            return this.removeById(instance);
        } else {
            return SqlHelper.retBool(this.getBaseMapper().deleteById(id));
        }
    }

    @Override
    public boolean removeBatchByIds(Collection<?> list, int batchSize) {
        TableInfo tableInfo = TableInfoHelper.getTableInfo(this.entityClass);
        return this.removeBatchByIds(list, batchSize, tableInfo.isWithLogicDelete() && tableInfo.isWithUpdateFill());
    }

    @Override
    public boolean updateBatchById(Collection<T> entityList, int batchSize) {
        return false;
    }

}
