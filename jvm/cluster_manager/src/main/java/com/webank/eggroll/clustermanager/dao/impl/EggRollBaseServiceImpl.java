package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
//import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import java.util.Collection;
import java.util.List;

public class EggRollBaseServiceImpl<M extends BaseMapper<T>, T> extends ServiceImpl<M, T> {
    public List<T> list(T entity) {
        return this.list(new QueryWrapper<>(entity));
    }

    public <E extends IPage<T>> E page(E page, T entity) {
        return this.page(page, new QueryWrapper<>(entity));
    }

    public T get(T entity) {
        return this.getOne(new QueryWrapper<>(entity));
    }


    //guice的原生saveBatch没有效果
    public boolean saveBatch(Collection<T> entityList) {
        if(entityList !=null){
            entityList.forEach(this::save);
        }
        return true;
    }
}
