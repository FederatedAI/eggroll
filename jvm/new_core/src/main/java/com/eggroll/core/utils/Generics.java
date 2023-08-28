package com.eggroll.core.utils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class Generics {

  /**
   * 查找类对象clazz绑定的genericClass声明的泛型参数
   *
   * @param clazz        绑定泛型参数的类
   * @param genericClass 声明泛型的类

   * @return 如果绑定了泛型参数, 则返回泛型类型, 否则返回null
   */
  public static Class[] find(Class clazz, Class genericClass) {
    if (!genericClass.isAssignableFrom(clazz)) {
      return null;
    }
    List<Type> types = getGenericTypes(clazz);
    for (Type type : types) {
      Class[] genericType = find(type, genericClass);
      if (genericType != null) {
        return genericType;
      }
    }
    return null;
  }

  private  static Class[] getClasses(Type[] types){

    Class[] result =  new Class[types.length];
    for(int i=0;i<types.length;i++){

      Type realType=  types[i];
      if (realType instanceof Class) {
        result[i]=  (Class) realType;
      } else if (realType instanceof ParameterizedType) {
        //这里是泛型的泛型
        result[i] =  (Class) ((ParameterizedType) realType).getRawType();
      }
    }
    return result;
  }
  /**
   * 查找类type上绑定的genericClass声明的泛型类型
   *
   * @param type         泛型对象
   * @param genericClass 声明泛型的类

   * @return
   */
  private static Class[] find(Type type, Class genericClass) {
    if (type instanceof Class) {
      return find((Class) type, genericClass);
    }
    if (type instanceof ParameterizedType) {
      ParameterizedType pType = (ParameterizedType) type;
      Type rawType = pType.getRawType();
      if (rawType instanceof Class) {
        Class rawClass = (Class) rawType;
        if (rawClass == genericClass) {
          Type[] types = pType.getActualTypeArguments();
          return  getClasses(types);
//          Type realType = pType.getActualTypeArguments()[index];
//          if (realType instanceof Class) {
//            return (Class) realType;
//          } else if (realType instanceof ParameterizedType) {
//            //这里是泛型的泛型
//            return (Class) ((ParameterizedType) realType).getRawType();
//          }



        } else if (genericClass.isAssignableFrom(rawClass)) {
          Map<String, Type> map = combine(pType.getActualTypeArguments(), rawClass);
          return find(rawClass, genericClass, map);
        }
      }
    }
    return null;
  }

  /**
   * 查找类currentClass上绑定的genericClass声明的泛型类型
   *
   * @param currentClass 绑定泛型参数的类
   * @param genericClass 声明泛型的类

   * @param typeMap      已绑定的泛型类型映射表
   * @return
   */
  private static Class[] find(Class currentClass, Class genericClass, Map<String, Type> typeMap) {
    List<Type> types = getGenericTypes(currentClass);
    for (Type type : types) {
      if (type instanceof ParameterizedType) {
        ParameterizedType pType = (ParameterizedType) type;
        Type rawType = pType.getRawType();
        if (rawType instanceof Class) {
          Class rawClass = (Class) rawType;
          Map<String, Type> map = transfer(pType, typeMap);
          Type[] typeArray = map.values().toArray(new Type[0]);
          if (rawClass == genericClass) {

            return  getClasses(typeArray);
//            Type realType = typeArray[index];
//            if (realType instanceof Class) {
//              return (Class) realType;
//            } else if (realType instanceof ParameterizedType) {
//              //这里是泛型的泛型
//              return (Class) ((ParameterizedType) realType).getRawType();
//            }
          } else if (genericClass.isAssignableFrom(rawClass)) {
            return find(rawClass, genericClass, combine(typeArray, rawClass));
          }
        }
      }
    }
    return null;
  }


  /**
   * 获取当前类继承的父类和实现接口的泛型列表
   *
   * @param clazz
   * @return
   */
  private static List<Type> getGenericTypes(Class clazz) {
    if (clazz == Object.class) {
      return Collections.EMPTY_LIST;
    }
    List<Type> list = new ArrayList<>();
    Type[] types = clazz.getGenericInterfaces();
    for (Type type : types) {
      list.add(type);
    }
    list.add(clazz.getGenericSuperclass());
    return list;
  }

  /**
   * 构建泛型映射表
   *
   * @param typeArguments 泛型参数
   * @param rawClass      声明泛型的类
   * @return
   */
  private static Map<String, Type> combine(Type[] typeArguments, Class rawClass) {
    Map<String, Type> map = new LinkedHashMap<>(typeArguments.length);
    TypeVariable[] typeParameters = rawClass.getTypeParameters();
    for (int i = 0; i < typeParameters.length; i++) {
      map.put(typeParameters[i].getName(), typeArguments[i]);
    }
    return map;
  }


  /**
   * 转换泛型映射表
   *
   * @param parameterizedType 绑定参数类型的泛型对象
   * @param typeMap           已绑定的泛型类型映射表
   * @return
   */
  private static Map<String, Type> transfer(ParameterizedType parameterizedType, Map<String, Type> typeMap) {

    Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
    TypeVariable[] typeParameters = ((Class) parameterizedType.getRawType()).getTypeParameters();
    Map<String, Type> map = new LinkedHashMap<>();
    for (int i = 0; i < actualTypeArguments.length; i++) {
      Type type = actualTypeArguments[i];
      if (type instanceof TypeVariable) {
        String variableName = ((TypeVariable) type).getName();
        map.put(variableName, typeMap.get(variableName));
      } else {
        map.put(typeParameters[i].getName(), type);
      }
    }
    return map;
  }
}