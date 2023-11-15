package com.webank.eggroll.guice.module;

import com.baomidou.mybatisplus.core.toolkit.reflect.IGenericTypeResolver;
import com.baomidou.mybatisplus.core.toolkit.reflect.SpringReflectionHelper;
import org.fedai.eggroll.core.utils.Generics;
import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
import com.webank.eggroll.clustermanager.dao.impl.ServiceImpl;
import org.apache.commons.lang3.ArrayUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.List;

public class MybatisPlusUtil implements IGenericTypeResolver {

    static Class<?>[] extractModelClass(Class<?> mapperClass) {

        List<Class<?>> result = new ArrayList<Class<?>>();
        Type[] types = mapperClass.getGenericInterfaces();
        if (types.length == 0) {
            types = new Type[]{mapperClass.getGenericSuperclass()};

        }
        ParameterizedType target = null;
        Type[] var4 = types;
        int var5 = types.length;

        for (int var6 = 0; var6 < var5; ++var6) {
            Type type = var4[var6];
            if (type instanceof ParameterizedType) {
                Type[] typeArray = ((ParameterizedType) type).getActualTypeArguments();
                if (ArrayUtils.isNotEmpty(typeArray)) {


                    for (int i = 0; i < typeArray.length; ++i) {

                        Type t = typeArray[i];
                        if (!(t instanceof TypeVariable) && !(t instanceof WildcardType)) {
                            target = (ParameterizedType) type;
                            result.add((Class) target.getActualTypeArguments()[0]);
                        }

                    }
                    break;
                }
            }


        }

        Class<?>[] finalResult = new Class<?>[result.size()];
        return result.toArray(finalResult);
    }

    @Override
    public Class<?>[] resolveTypeArguments(Class<?> clazz, Class<?> genericIfc) {
        return Generics.find(clazz, genericIfc);
    }

    public static void main(String[] args) {

        Class<?>[] xxs = SpringReflectionHelper.resolveTypeArguments(NodeResourceService.class, ServiceImpl.class);
        for (int i = 0; i < xxs.length; i++) {
            System.err.println(xxs[i]);
        }
        System.err.println("====================");
//        xxs=extractModelClass(NodeResourceService.class)
        Class<?>[] xx1 = extractModelClass(NodeResourceService.class);
        for (int i = 0; i < xx1.length; i++) {
            System.err.println(xx1[i]);
        }

        System.err.println("===================");

        Class<?>[] xx2 = Generics.find(NodeResourceService.class, ServiceImpl.class);
        for (int i = 0; i < xx2.length; i++) {
            System.err.println(xx2[i]);
        }
    }

}
