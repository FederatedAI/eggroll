////
//// Source code recreated from a .class file by IntelliJ IDEA
//// (powered by FernFlower decompiler)
////
//
//package com.eggroll.core.utils;
//
//import java.io.Serializable;
//import java.lang.reflect.Array;
//import java.lang.reflect.Constructor;
//import java.lang.reflect.Field;
//import java.lang.reflect.GenericArrayType;
//import java.lang.reflect.Method;
//import java.lang.reflect.ParameterizedType;
//import java.lang.reflect.Type;
//import java.lang.reflect.TypeVariable;
//import java.lang.reflect.WildcardType;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.IdentityHashMap;
//import java.util.Map;
//import java.util.StringJoiner;
//
//
//public class ResolvableType implements Serializable {
//    public static final ResolvableType NONE;
//    private static final ResolvableType[] EMPTY_TYPES_ARRAY;
//    private static final ConcurrentReferenceHashMap<ResolvableType, ResolvableType> cache;
//    private final Type type;
//
////    private final TypeProvider typeProvider;
////    private final ResolvableType.VariableResolver variableResolver;
//    private final ResolvableType componentType;
//
//    private final Integer hash;
//
//    private Class<?> resolved;
//
//    private volatile ResolvableType superType;
//
//    private volatile ResolvableType[] interfaces;
//
//    private volatile ResolvableType[] generics;
//
////    private ResolvableType(Type type, TypeProvider typeProvider,  ResolvableType.VariableResolver variableResolver) {
////        this.type = type;
////        this.typeProvider = typeProvider;
////        this.variableResolver = variableResolver;
////        this.componentType = null;
////        this.hash = this.calculateHashCode();
////        this.resolved = null;
////    }
//
////    private ResolvableType(Type type, @Nullable TypeProvider typeProvider, @Nullable ResolvableType.VariableResolver variableResolver, @Nullable Integer hash) {
////        this.type = type;
////        this.typeProvider = typeProvider;
////        this.variableResolver = variableResolver;
////        this.componentType = null;
////        this.hash = hash;
////        this.resolved = this.resolveClass();
////    }
//
////    private ResolvableType(Type type, @Nullable TypeProvider typeProvider, @Nullable ResolvableType.VariableResolver variableResolver, @Nullable ResolvableType componentType) {
////        this.type = type;
////        this.typeProvider = typeProvider;
////        this.variableResolver = variableResolver;
////        this.componentType = componentType;
////        this.hash = null;
////        this.resolved = this.resolveClass();
////    }
//
//
//    public static ResolvableType forType( Type type) {
//        return forType(type, (TypeProvider)null, (ResolvableType.VariableResolver)null);
//    }
//
//
//    private ResolvableType( Class<?> clazz) {
//        this.resolved = clazz != null ? clazz : Object.class;
//        this.type = this.resolved;
////        this.typeProvider = null;
//  //      this.variableResolver = null;
//        this.componentType = null;
//        this.hash = null;
//    }
//
//    public Type getType() {
//        return SerializableTypeWrapper.unwrap(this.type);
//    }
//
//
//    public Class<?> getRawClass() {
//        if (this.type == this.resolved) {
//            return this.resolved;
//        } else {
//            Type rawType = this.type;
//            if (rawType instanceof ParameterizedType) {
//                rawType = ((ParameterizedType)rawType).getRawType();
//            }
//
//            return rawType instanceof Class ? (Class)rawType : null;
//        }
//    }
//
////    public Object getSource() {
////        Object source = this.typeProvider != null ? this.typeProvider.getSource() : null;
////        return source != null ? source : this.type;
////    }
//
//    public Class<?> toClass() {
//        return this.resolve(Object.class);
//    }
//
////    public boolean isInstance(@Nullable Object obj) {
////        return obj != null && this.isAssignableFrom(obj.getClass());
////    }
//
//    public boolean isAssignableFrom(Class<?> other) {
//        return this.isAssignableFrom(forClass(other), (Map)null);
//    }
//
//    public boolean isAssignableFrom(ResolvableType other) {
//        return this.isAssignableFrom(other, (Map)null);
//    }
//
////    private boolean isAssignableFrom(ResolvableType other, @Nullable Map<Type, Type> matchedBefore) {
////        Assert.notNull(other, "ResolvableType must not be null");
////        if (this != NONE && other != NONE) {
////            if (this.isArray()) {
////                return other.isArray() && this.getComponentType().isAssignableFrom(other.getComponentType());
////            } else if (matchedBefore != null && ((Map)matchedBefore).get(this.type) == other.type) {
////                return true;
////            } else {
////                ResolvableType.WildcardBounds ourBounds = ResolvableType.WildcardBounds.get(this);
////                ResolvableType.WildcardBounds typeBounds = ResolvableType.WildcardBounds.get(other);
////                if (typeBounds != null) {
////                    return ourBounds != null && ourBounds.isSameKind(typeBounds) && ourBounds.isAssignableFrom(typeBounds.getBounds());
////                } else if (ourBounds != null) {
////                    return ourBounds.isAssignableFrom(other);
////                } else {
////                    boolean exactMatch = matchedBefore != null;
////                    boolean checkGenerics = true;
////                    Class<?> ourResolved = null;
////                    if (this.type instanceof TypeVariable) {
////                        TypeVariable<?> variable = (TypeVariable)this.type;
////                        ResolvableType resolved;
////                        if (this.variableResolver != null) {
////                            resolved = this.variableResolver.resolveVariable(variable);
////                            if (resolved != null) {
////                                ourResolved = resolved.resolve();
////                            }
////                        }
////
////                        if (ourResolved == null && other.variableResolver != null) {
////                            resolved = other.variableResolver.resolveVariable(variable);
////                            if (resolved != null) {
////                                ourResolved = resolved.resolve();
////                                checkGenerics = false;
////                            }
////                        }
////
////                        if (ourResolved == null) {
////                            exactMatch = false;
////                        }
////                    }
////
////                    if (ourResolved == null) {
////                        ourResolved = this.resolve(Object.class);
////                    }
////
////                    Class<?> otherResolved = other.toClass();
////                    if (exactMatch) {
////                        if (!ourResolved.equals(otherResolved)) {
////                            return false;
////                        }
////                    } else if (!ClassUtils.isAssignable(ourResolved, otherResolved)) {
////                        return false;
////                    }
////
////                    if (checkGenerics) {
////                        ResolvableType[] ourGenerics = this.getGenerics();
////                        ResolvableType[] typeGenerics = other.as(ourResolved).getGenerics();
////                        if (ourGenerics.length != typeGenerics.length) {
////                            return false;
////                        }
////
////                        if (matchedBefore == null) {
////                            matchedBefore = new IdentityHashMap(1);
////                        }
////
////                        ((Map)matchedBefore).put(this.type, other.type);
////
////                        for(int i = 0; i < ourGenerics.length; ++i) {
////                            if (!ourGenerics[i].isAssignableFrom(typeGenerics[i], (Map)matchedBefore)) {
////                                return false;
////                            }
////                        }
////                    }
////
////                    return true;
////                }
////            }
////        } else {
////            return false;
////        }
////    }
//
//    public boolean isArray() {
//        if (this == NONE) {
//            return false;
//        } else {
//            return this.type instanceof Class && ((Class)this.type).isArray() || this.type instanceof GenericArrayType || this.resolveType().isArray();
//        }
//    }
//
////    public ResolvableType getComponentType() {
////        if (this == NONE) {
////            return NONE;
////        } else if (this.componentType != null) {
////            return this.componentType;
////        } else if (this.type instanceof Class) {
////            Class<?> componentType = ((Class)this.type).getComponentType();
////            return forType(componentType, (ResolvableType.VariableResolver)this.variableResolver);
////        } else {
////            return this.type instanceof GenericArrayType ? forType(((GenericArrayType)this.type).getGenericComponentType(), this.variableResolver) : this.resolveType().getComponentType();
////        }
////    }
//
////    public ResolvableType asCollection() {
////        return this.as(Collection.class);
////    }
////
////    public ResolvableType asMap() {
////        return this.as(Map.class);
////    }
//
//    public ResolvableType as(Class<?> type) {
//        if (this == NONE) {
//            return NONE;
//        } else {
//            Class<?> resolved = this.resolve();
//            if (resolved != null && resolved != type) {
//                ResolvableType[] var3 = this.getInterfaces();
//                int var4 = var3.length;
//
//                for(int var5 = 0; var5 < var4; ++var5) {
//                    ResolvableType interfaceType = var3[var5];
//                    ResolvableType interfaceAsType = interfaceType.as(type);
//                    if (interfaceAsType != NONE) {
//                        return interfaceAsType;
//                    }
//                }
//
//                return this.getSuperType().as(type);
//            } else {
//                return this;
//            }
//        }
//    }
//
//    public ResolvableType getSuperType() {
//        Class<?> resolved = this.resolve();
//        if (resolved == null) {
//            return NONE;
//        } else {
//            try {
//                Type superclass = resolved.getGenericSuperclass();
//                if (superclass == null) {
//                    return NONE;
//                } else {
//                    ResolvableType superType = this.superType;
//                    if (superType == null) {
//                        superType = forType(superclass, this);
//                        this.superType = superType;
//                    }
//
//                    return superType;
//                }
//            } catch (TypeNotPresentException var4) {
//                return NONE;
//            }
//        }
//    }
//
//    public ResolvableType[] getInterfaces() {
//        Class<?> resolved = this.resolve();
//        if (resolved == null) {
//            return EMPTY_TYPES_ARRAY;
//        } else {
//            ResolvableType[] interfaces = this.interfaces;
//            if (interfaces == null) {
//                Type[] genericIfcs = resolved.getGenericInterfaces();
//                interfaces = new ResolvableType[genericIfcs.length];
//
//                for(int i = 0; i < genericIfcs.length; ++i) {
//                    interfaces[i] = forType(genericIfcs[i], this);
//                }
//
//                this.interfaces = interfaces;
//            }
//
//            return interfaces;
//        }
//    }
//
//    public boolean hasGenerics() {
//        return this.getGenerics().length > 0;
//    }
//
//    boolean isEntirelyUnresolvable() {
//        if (this == NONE) {
//            return false;
//        } else {
//            ResolvableType[] generics = this.getGenerics();
//            ResolvableType[] var2 = generics;
//            int var3 = generics.length;
//
//            for(int var4 = 0; var4 < var3; ++var4) {
//                ResolvableType generic = var2[var4];
//                if (!generic.isUnresolvableTypeVariable() && !generic.isWildcardWithoutBounds()) {
//                    return false;
//                }
//            }
//
//            return true;
//        }
//    }
//
//    public boolean hasUnresolvableGenerics() {
//        if (this == NONE) {
//            return false;
//        } else {
//            ResolvableType[] generics = this.getGenerics();
//            ResolvableType[] var2 = generics;
//            int var3 = generics.length;
//
//            int var4;
//            for(var4 = 0; var4 < var3; ++var4) {
//                ResolvableType generic = var2[var4];
//                if (generic.isUnresolvableTypeVariable() || generic.isWildcardWithoutBounds()) {
//                    return true;
//                }
//            }
//
//            Class<?> resolved = this.resolve();
//            if (resolved != null) {
//                try {
//                    Type[] var9 = resolved.getGenericInterfaces();
//                    var4 = var9.length;
//
//                    for(int var10 = 0; var10 < var4; ++var10) {
//                        Type genericInterface = var9[var10];
//                        if (genericInterface instanceof Class && forClass((Class)genericInterface).hasGenerics()) {
//                            return true;
//                        }
//                    }
//                } catch (TypeNotPresentException var7) {
//                }
//
//                return this.getSuperType().hasUnresolvableGenerics();
//            } else {
//                return false;
//            }
//        }
//    }
//
//    private boolean isUnresolvableTypeVariable() {
//        if (this.type instanceof TypeVariable) {
//            if (this.variableResolver == null) {
//                return true;
//            }
//
//            TypeVariable<?> variable = (TypeVariable)this.type;
//            ResolvableType resolved = this.variableResolver.resolveVariable(variable);
//            if (resolved == null || resolved.isUnresolvableTypeVariable()) {
//                return true;
//            }
//        }
//
//        return false;
//    }
//
//    private boolean isWildcardWithoutBounds() {
//        if (this.type instanceof WildcardType) {
//            WildcardType wt = (WildcardType)this.type;
//            if (wt.getLowerBounds().length == 0) {
//                Type[] upperBounds = wt.getUpperBounds();
//                if (upperBounds.length == 0 || upperBounds.length == 1 && Object.class == upperBounds[0]) {
//                    return true;
//                }
//            }
//        }
//
//        return false;
//    }
//
////    public ResolvableType getNested(int nestingLevel) {
////        return this.getNested(nestingLevel, (Map)null);
////    }
//
////    public ResolvableType getNested(int nestingLevel, @Nullable Map<Integer, Integer> typeIndexesPerLevel) {
////        ResolvableType result = this;
////
////        for(int i = 2; i <= nestingLevel; ++i) {
////            if (result.isArray()) {
////                result = result.getComponentType();
////            } else {
////                while(result != NONE && !result.hasGenerics()) {
////                    result = result.getSuperType();
////                }
////
////                Integer index = typeIndexesPerLevel != null ? (Integer)typeIndexesPerLevel.get(i) : null;
////                index = index == null ? result.getGenerics().length - 1 : index;
////                result = result.getGeneric(index);
////            }
////        }
////
////        return result;
////    }
//
//    public ResolvableType getGeneric( int... indexes) {
//        ResolvableType[] generics = this.getGenerics();
//        if (indexes != null && indexes.length != 0) {
//            ResolvableType generic = this;
//            int[] var4 = indexes;
//            int var5 = indexes.length;
//
//            for(int var6 = 0; var6 < var5; ++var6) {
//                int index = var4[var6];
//                generics = generic.getGenerics();
//                if (index < 0 || index >= generics.length) {
//                    return NONE;
//                }
//
//                generic = generics[index];
//            }
//
//            return generic;
//        } else {
//            return generics.length == 0 ? NONE : generics[0];
//        }
//    }
//
//    public ResolvableType[] getGenerics() {
//        if (this == NONE) {
//            return EMPTY_TYPES_ARRAY;
//        } else {
//            ResolvableType[] generics = this.generics;
//            if (generics == null) {
//                int i;
//                if (this.type instanceof Class) {
//                    Type[] typeParams = ((Class)this.type).getTypeParameters();
//                    generics = new ResolvableType[typeParams.length];
//
//                    for(i = 0; i < generics.length; ++i) {
//                        generics[i] = forType(typeParams[i], (ResolvableType)this);
//                    }
//                } else if (this.type instanceof ParameterizedType) {
//                    Type[] actualTypeArguments = ((ParameterizedType)this.type).getActualTypeArguments();
//                    generics = new ResolvableType[actualTypeArguments.length];
//
//                    for(i = 0; i < actualTypeArguments.length; ++i) {
//                        generics[i] = forType(actualTypeArguments[i], this.variableResolver);
//                    }
//                } else {
//                    generics = this.resolveType().getGenerics();
//                }
//
//                this.generics = generics;
//            }
//
//            return generics;
//        }
//    }
//
//    public Class<?>[] resolveGenerics() {
//        ResolvableType[] generics = this.getGenerics();
//        Class<?>[] resolvedGenerics = new Class[generics.length];
//
//        for(int i = 0; i < generics.length; ++i) {
//            resolvedGenerics[i] = generics[i].resolve();
//        }
//
//        return resolvedGenerics;
//    }
//
//    public Class<?>[] resolveGenerics(Class<?> fallback) {
//        ResolvableType[] generics = this.getGenerics();
//        Class<?>[] resolvedGenerics = new Class[generics.length];
//
//        for(int i = 0; i < generics.length; ++i) {
//            resolvedGenerics[i] = generics[i].resolve(fallback);
//        }
//
//        return resolvedGenerics;
//    }
//
//
//    public Class<?> resolveGeneric(int... indexes) {
//        return this.getGeneric(indexes).resolve();
//    }
//
//    public Class<?> resolve() {
//        return this.resolved;
//    }
//
//    public Class<?> resolve(Class<?> fallback) {
//        return this.resolved != null ? this.resolved : fallback;
//    }
//
//
//    private Class<?> resolveClass() {
//        if (this.type == ResolvableType.EmptyType.INSTANCE) {
//            return null;
//        } else if (this.type instanceof Class) {
//            return (Class)this.type;
//        } else if (this.type instanceof GenericArrayType) {
//            Class<?> resolvedComponent = this.getComponentType().resolve();
//            return resolvedComponent != null ? Array.newInstance(resolvedComponent, 0).getClass() : null;
//        } else {
//            return this.resolveType().resolve();
//        }
//    }
//
//    ResolvableType resolveType() {
//        if (this.type instanceof ParameterizedType) {
//            return forType(((ParameterizedType)this.type).getRawType(), this.variableResolver);
//        } else if (this.type instanceof WildcardType) {
//            Type resolved = this.resolveBounds(((WildcardType)this.type).getUpperBounds());
//            if (resolved == null) {
//                resolved = this.resolveBounds(((WildcardType)this.type).getLowerBounds());
//            }
//
//            return forType(resolved, this.variableResolver);
//        } else if (this.type instanceof TypeVariable) {
//            TypeVariable<?> variable = (TypeVariable)this.type;
//            if (this.variableResolver != null) {
//                ResolvableType resolved = this.variableResolver.resolveVariable(variable);
//                if (resolved != null) {
//                    return resolved;
//                }
//            }
//
//            return forType(this.resolveBounds(variable.getBounds()), this.variableResolver);
//        } else {
//            return NONE;
//        }
//    }
//
//
//    private Type resolveBounds(Type[] bounds) {
//        return bounds.length != 0 && bounds[0] != Object.class ? bounds[0] : null;
//    }
//
//    private ResolvableType resolveVariable(TypeVariable<?> variable) {
//        if (this.type instanceof TypeVariable) {
//            return this.resolveType().resolveVariable(variable);
//        } else {
//            if (this.type instanceof ParameterizedType) {
//                ParameterizedType parameterizedType = (ParameterizedType)this.type;
//                Class<?> resolved = this.resolve();
//                if (resolved == null) {
//                    return null;
//                }
//
//                TypeVariable<?>[] variables = resolved.getTypeParameters();
//
//                for(int i = 0; i < variables.length; ++i) {
//                    if (ObjectUtils.nullSafeEquals(variables[i].getName(), variable.getName())) {
//                        Type actualType = parameterizedType.getActualTypeArguments()[i];
//                        return forType(actualType, this.variableResolver);
//                    }
//                }
//
//                Type ownerType = parameterizedType.getOwnerType();
//                if (ownerType != null) {
//                    return forType(ownerType, this.variableResolver).resolveVariable(variable);
//                }
//            }
//
//            if (this.type instanceof WildcardType) {
//                ResolvableType resolved = this.resolveType().resolveVariable(variable);
//                if (resolved != null) {
//                    return resolved;
//                }
//            }
//
//            return this.variableResolver != null ? this.variableResolver.resolveVariable(variable) : null;
//        }
//    }
//
////    public boolean equals(@Nullable Object other) {
////        if (this == other) {
////            return true;
////        } else if (!(other instanceof ResolvableType)) {
////            return false;
////        } else {
////            ResolvableType otherType = (ResolvableType)other;
////            if (!ObjectUtils.nullSafeEquals(this.type, otherType.type)) {
////                return false;
////            } else if (this.typeProvider != otherType.typeProvider && (this.typeProvider == null || otherType.typeProvider == null || !ObjectUtils.nullSafeEquals(this.typeProvider.getType(), otherType.typeProvider.getType()))) {
////                return false;
////            } else if (this.variableResolver == otherType.variableResolver || this.variableResolver != null && otherType.variableResolver != null && ObjectUtils.nullSafeEquals(this.variableResolver.getSource(), otherType.variableResolver.getSource())) {
////                return ObjectUtils.nullSafeEquals(this.componentType, otherType.componentType);
////            } else {
////                return false;
////            }
////        }
////    }
//
//    public int hashCode() {
//        return this.hash != null ? this.hash : this.calculateHashCode();
//    }
//
////    private int calculateHashCode() {
////        int hashCode = ObjectUtils.nullSafeHashCode(this.type);
////        if (this.typeProvider != null) {
////            hashCode = 31 * hashCode + ObjectUtils.nullSafeHashCode(this.typeProvider.getType());
////        }
////
////        if (this.variableResolver != null) {
////            hashCode = 31 * hashCode + ObjectUtils.nullSafeHashCode(this.variableResolver.getSource());
////        }
////
////        if (this.componentType != null) {
////            hashCode = 31 * hashCode + ObjectUtils.nullSafeHashCode(this.componentType);
////        }
////
////        return hashCode;
////    }
//
//
//    ResolvableType.VariableResolver asVariableResolver() {
//        return this == NONE ? null : new ResolvableType.DefaultVariableResolver(this);
//    }
//
//    private Object readResolve() {
//        return this.type == ResolvableType.EmptyType.INSTANCE ? NONE : this;
//    }
//
//    public String toString() {
//        if (this.isArray()) {
//            return this.getComponentType() + "[]";
//        } else if (this.resolved == null) {
//            return "?";
//        } else {
//            if (this.type instanceof TypeVariable) {
//                TypeVariable<?> variable = (TypeVariable)this.type;
//                if (this.variableResolver == null || this.variableResolver.resolveVariable(variable) == null) {
//                    return "?";
//                }
//            }
//
//            return this.hasGenerics() ? this.resolved.getName() + '<' + StringUtils.arrayToDelimitedString(this.getGenerics(), ", ") + '>' : this.resolved.getName();
//        }
//    }
//
//    public static ResolvableType forClass( Class<?> clazz) {
//        return new ResolvableType(clazz);
//    }
//
////    public static ResolvableType forRawClass(@Nullable Class<?> clazz) {
////        return new ResolvableType(clazz) {
////            public ResolvableType[] getGenerics() {
////                return ResolvableType.EMPTY_TYPES_ARRAY;
////            }
////
////            public boolean isAssignableFrom(Class<?> other) {
////                return clazz == null || ClassUtils.isAssignable(clazz, other);
////            }
////
////            public boolean isAssignableFrom(ResolvableType other) {
////                Class<?> otherClass = other.resolve();
////                return otherClass != null && (clazz == null || ClassUtils.isAssignable(clazz, otherClass));
////            }
////        };
////    }
//
////    public static ResolvableType forClass(Class<?> baseType, Class<?> implementationClass) {
////        Assert.notNull(baseType, "Base type must not be null");
////        ResolvableType asType = forType((Type)implementationClass).as(baseType);
////        return asType == NONE ? forType((Type)baseType) : asType;
////    }
////
////    public static ResolvableType forClassWithGenerics(Class<?> clazz, Class<?>... generics) {
////        Assert.notNull(clazz, "Class must not be null");
////        Assert.notNull(generics, "Generics array must not be null");
////        ResolvableType[] resolvableGenerics = new ResolvableType[generics.length];
////
////        for(int i = 0; i < generics.length; ++i) {
////            resolvableGenerics[i] = forClass(generics[i]);
////        }
////
////        return forClassWithGenerics(clazz, resolvableGenerics);
////    }
//
////    public static ResolvableType forClassWithGenerics(Class<?> clazz, ResolvableType... generics) {
////        Assert.notNull(clazz, "Class must not be null");
////        Assert.notNull(generics, "Generics array must not be null");
////        TypeVariable<?>[] variables = clazz.getTypeParameters();
////        Assert.isTrue(variables.length == generics.length, () -> {
////            return "Mismatched number of generics specified for " + clazz.toGenericString();
////        });
////        Type[] arguments = new Type[generics.length];
////
////        for(int i = 0; i < generics.length; ++i) {
////            ResolvableType generic = generics[i];
////            Type argument = generic != null ? generic.getType() : null;
////            arguments[i] = (Type)(argument != null && !(argument instanceof TypeVariable) ? argument : variables[i]);
////        }
////
////        ParameterizedType syntheticType = new ResolvableType.SyntheticParameterizedType(clazz, arguments);
////        return forType(syntheticType, (ResolvableType.VariableResolver)(new ResolvableType.TypeVariablesVariableResolver(variables, generics)));
////    }
//
////    public static ResolvableType forInstance(@Nullable Object instance) {
////        if (instance instanceof ResolvableTypeProvider) {
////            ResolvableType type = ((ResolvableTypeProvider)instance).getResolvableType();
////            if (type != null) {
////                return type;
////            }
////        }
////
////        return instance != null ? forClass(instance.getClass()) : NONE;
////    }
//
////    public static ResolvableType forField(Field field) {
////        Assert.notNull(field, "Field must not be null");
////        return forType((Type)null, new FieldTypeProvider(field), (ResolvableType.VariableResolver)null);
////    }
//
////    public static ResolvableType forField(Field field, Class<?> implementationClass) {
////        Assert.notNull(field, "Field must not be null");
////        ResolvableType owner = forType((Type)implementationClass).as(field.getDeclaringClass());
////        return forType((Type)null, new FieldTypeProvider(field), owner.asVariableResolver());
////    }
//
////    public static ResolvableType forField(Field field, @Nullable ResolvableType implementationType) {
////        Assert.notNull(field, "Field must not be null");
////        ResolvableType owner = implementationType != null ? implementationType : NONE;
////        owner = owner.as(field.getDeclaringClass());
////        return forType((Type)null, new FieldTypeProvider(field), owner.asVariableResolver());
////    }
//
////    public static ResolvableType forField(Field field, int nestingLevel) {
////        Assert.notNull(field, "Field must not be null");
////        return forType((Type)null, new FieldTypeProvider(field), (ResolvableType.VariableResolver)null).getNested(nestingLevel);
////    }
//
////    public static ResolvableType forField(Field field, int nestingLevel, @Nullable Class<?> implementationClass) {
////        Assert.notNull(field, "Field must not be null");
////        ResolvableType owner = forType((Type)implementationClass).as(field.getDeclaringClass());
////        return forType((Type)null, new FieldTypeProvider(field), owner.asVariableResolver()).getNested(nestingLevel);
////    }
////
////    public static ResolvableType forConstructorParameter(Constructor<?> constructor, int parameterIndex) {
////        Assert.notNull(constructor, "Constructor must not be null");
////        return forMethodParameter(new MethodParameter(constructor, parameterIndex));
////    }
////
////    public static ResolvableType forConstructorParameter(Constructor<?> constructor, int parameterIndex, Class<?> implementationClass) {
////        Assert.notNull(constructor, "Constructor must not be null");
////        MethodParameter methodParameter = new MethodParameter(constructor, parameterIndex, implementationClass);
////        return forMethodParameter(methodParameter);
////    }
////
////    public static ResolvableType forMethodReturnType(Method method) {
////        Assert.notNull(method, "Method must not be null");
////        return forMethodParameter(new MethodParameter(method, -1));
////    }
////
////    public static ResolvableType forMethodReturnType(Method method, Class<?> implementationClass) {
////        Assert.notNull(method, "Method must not be null");
////        MethodParameter methodParameter = new MethodParameter(method, -1, implementationClass);
////        return forMethodParameter(methodParameter);
////    }
////
////    public static ResolvableType forMethodParameter(Method method, int parameterIndex) {
////        Assert.notNull(method, "Method must not be null");
////        return forMethodParameter(new MethodParameter(method, parameterIndex));
////    }
////
////    public static ResolvableType forMethodParameter(Method method, int parameterIndex, Class<?> implementationClass) {
////        Assert.notNull(method, "Method must not be null");
////        MethodParameter methodParameter = new MethodParameter(method, parameterIndex, implementationClass);
////        return forMethodParameter(methodParameter);
////    }
////
////    public static ResolvableType forMethodParameter(MethodParameter methodParameter) {
////        return forMethodParameter(methodParameter, (Type)null);
////    }
//
////    public static ResolvableType forMethodParameter(MethodParameter methodParameter, @Nullable ResolvableType implementationType) {
//////        Assert.notNull(methodParameter, "MethodParameter must not be null");
//////        implementationType = implementationType != null ? implementationType : forType((Type)methodParameter.getContainingClass());
//////        ResolvableType owner = implementationType.as(methodParameter.getDeclaringClass());
//////        return forType((Type)null, new MethodParameterTypeProvider(methodParameter), owner.asVariableResolver()).getNested(methodParameter.getNestingLevel(), methodParameter.typeIndexesPerLevel);
//////    }
//////
//////    public static ResolvableType forMethodParameter(MethodParameter methodParameter, @Nullable Type targetType) {
//////        Assert.notNull(methodParameter, "MethodParameter must not be null");
//////        return forMethodParameter(methodParameter, targetType, methodParameter.getNestingLevel());
//////    }
//////
//////    static ResolvableType forMethodParameter(MethodParameter methodParameter, @Nullable Type targetType, int nestingLevel) {
//////        ResolvableType owner = forType((Type)methodParameter.getContainingClass()).as(methodParameter.getDeclaringClass());
//////        return forType(targetType, new MethodParameterTypeProvider(methodParameter), owner.asVariableResolver()).getNested(nestingLevel, methodParameter.typeIndexesPerLevel);
//////    }
//////
//////    public static ResolvableType forArrayComponent(ResolvableType componentType) {
//////        Assert.notNull(componentType, "Component type must not be null");
//////        Class<?> arrayClass = Array.newInstance(componentType.resolve(), 0).getClass();
//////        return new ResolvableType(arrayClass, (TypeProvider)null, (ResolvableType.VariableResolver)null, componentType);
//////    }
//////
//////    public static ResolvableType forType(@Nullable Type type) {
//////        return forType(type, (TypeProvider)null, (ResolvableType.VariableResolver)null);
//////    }
//////
//////    public static ResolvableType forType(@Nullable Type type, @Nullable ResolvableType owner) {
//////        ResolvableType.VariableResolver variableResolver = null;
//////        if (owner != null) {
//////            variableResolver = owner.asVariableResolver();
//////        }
//////
//////        return forType(type, variableResolver);
//////    }
//////
//////    public static ResolvableType forType(ParameterizedTypeReference<?> typeReference) {
//////        return forType(typeReference.getType(), (TypeProvider)null, (ResolvableType.VariableResolver)null);
//////    }
//////
//////    static ResolvableType forType(@Nullable Type type, @Nullable ResolvableType.VariableResolver variableResolver) {
//////        return forType(type, (TypeProvider)null, variableResolver);
//////    }
//////
//////    static ResolvableType forType(@Nullable Type type, @Nullable TypeProvider typeProvider, @Nullable ResolvableType.VariableResolver variableResolver) {
//////        if (type == null && typeProvider != null) {
//////            type = SerializableTypeWrapper.forTypeProvider(typeProvider);
//////        }
//////
//////        if (type == null) {
//////            return NONE;
//////        } else if (type instanceof Class) {
//////            return new ResolvableType(type, typeProvider, variableResolver, (ResolvableType)null);
//////        } else {
//////            cache.purgeUnreferencedEntries();
//////            ResolvableType resultType = new ResolvableType(type, typeProvider, variableResolver);
//////            ResolvableType cachedType = (ResolvableType)cache.get(resultType);
//////            if (cachedType == null) {
//////                cachedType = new ResolvableType(type, typeProvider, variableResolver, resultType.hash);
//////                cache.put(cachedType, cachedType);
//////            }
//////
//////            resultType.resolved = cachedType.resolved;
//////            return resultType;
//////        }
//////    }
//////
//////    public static void clearCache() {
//////        cache.clear();
//////        SerializableTypeWrapper.cache.clear();
//////    }
//
//    static {
//        NONE = new ResolvableType(ResolvableType.EmptyType.INSTANCE, (TypeProvider)null, (ResolvableType.VariableResolver)null, 0);
//        EMPTY_TYPES_ARRAY = new ResolvableType[0];
//        cache = new ConcurrentReferenceHashMap(256);
//    }
//
//    static class EmptyType implements Type, Serializable {
//        static final Type INSTANCE = new ResolvableType.EmptyType();
//
//        EmptyType() {
//        }
//
//        Object readResolve() {
//            return INSTANCE;
//        }
//    }
//
////    private static class WildcardBounds {
////        private final ResolvableType.WildcardBounds.Kind kind;
////        private final ResolvableType[] bounds;
////
////        public WildcardBounds(ResolvableType.WildcardBounds.Kind kind, ResolvableType[] bounds) {
////            this.kind = kind;
////            this.bounds = bounds;
////        }
////
////        public boolean isSameKind(ResolvableType.WildcardBounds bounds) {
////            return this.kind == bounds.kind;
////        }
////
////        public boolean isAssignableFrom(ResolvableType... types) {
////            ResolvableType[] var2 = this.bounds;
////            int var3 = var2.length;
////
////            for(int var4 = 0; var4 < var3; ++var4) {
////                ResolvableType bound = var2[var4];
////                ResolvableType[] var6 = types;
////                int var7 = types.length;
////
////                for(int var8 = 0; var8 < var7; ++var8) {
////                    ResolvableType type = var6[var8];
////                    if (!this.isAssignable(bound, type)) {
////                        return false;
////                    }
////                }
////            }
////
////            return true;
////        }
////
//////        private boolean isAssignable(ResolvableType source, ResolvableType from) {
//////            return this.kind == ResolvableType.WildcardBounds.Kind.UPPER ? source.isAssignableFrom(from) : from.isAssignableFrom(source);
//////        }
////
////        public ResolvableType[] getBounds() {
////            return this.bounds;
////        }
//
////        @Nullable
////        public static ResolvableType.WildcardBounds get(ResolvableType type) {
////            ResolvableType resolveToWildcard;
////            for(resolveToWildcard = type; !(resolveToWildcard.getType() instanceof WildcardType); resolveToWildcard = resolveToWildcard.resolveType()) {
////                if (resolveToWildcard == ResolvableType.NONE) {
////                    return null;
////                }
////            }
////
////            WildcardType wildcardType = (WildcardType)resolveToWildcard.type;
////            ResolvableType.WildcardBounds.Kind boundsType = wildcardType.getLowerBounds().length > 0 ? ResolvableType.WildcardBounds.Kind.LOWER : ResolvableType.WildcardBounds.Kind.UPPER;
////            Type[] bounds = boundsType == ResolvableType.WildcardBounds.Kind.UPPER ? wildcardType.getUpperBounds() : wildcardType.getLowerBounds();
////            ResolvableType[] resolvableBounds = new ResolvableType[bounds.length];
////
////            for(int i = 0; i < bounds.length; ++i) {
////                resolvableBounds[i] = ResolvableType.forType(bounds[i], type.variableResolver);
////            }
////
////            return new ResolvableType.WildcardBounds(boundsType, resolvableBounds);
////        }
////
////        static enum Kind {
////            UPPER,
////            LOWER;
////
////            private Kind() {
////            }
////        }
////    }
//
////    private static final class SyntheticParameterizedType implements ParameterizedType, Serializable {
////        private final Type rawType;
////        private final Type[] typeArguments;
////
////        public SyntheticParameterizedType(Type rawType, Type[] typeArguments) {
////            this.rawType = rawType;
////            this.typeArguments = typeArguments;
////        }
////
////        public String getTypeName() {
////            String typeName = this.rawType.getTypeName();
////            if (this.typeArguments.length <= 0) {
////                return typeName;
////            } else {
////                StringJoiner stringJoiner = new StringJoiner(", ", "<", ">");
////                Type[] var3 = this.typeArguments;
////                int var4 = var3.length;
////
////                for(int var5 = 0; var5 < var4; ++var5) {
////                    Type argument = var3[var5];
////                    stringJoiner.add(argument.getTypeName());
////                }
////
////                return typeName + stringJoiner;
////            }
////        }
////
////        @Nullable
////        public Type getOwnerType() {
////            return null;
////        }
////
////        public Type getRawType() {
////            return this.rawType;
////        }
////
////        public Type[] getActualTypeArguments() {
////            return this.typeArguments;
////        }
////
////        public boolean equals(@Nullable Object other) {
////            if (this == other) {
////                return true;
////            } else if (!(other instanceof ParameterizedType)) {
////                return false;
////            } else {
////                ParameterizedType otherType = (ParameterizedType)other;
////                return otherType.getOwnerType() == null && this.rawType.equals(otherType.getRawType()) && Arrays.equals(this.typeArguments, otherType.getActualTypeArguments());
////            }
////        }
////
////        public int hashCode() {
////            return this.rawType.hashCode() * 31 + Arrays.hashCode(this.typeArguments);
////        }
////
////        public String toString() {
////            return this.getTypeName();
////        }
////    }
//
////    private static class TypeVariablesVariableResolver implements ResolvableType.VariableResolver {
////        private final TypeVariable<?>[] variables;
////        private final ResolvableType[] generics;
////
////        public TypeVariablesVariableResolver(TypeVariable<?>[] variables, ResolvableType[] generics) {
////            this.variables = variables;
////            this.generics = generics;
////        }
////
////        @Nullable
////        public ResolvableType resolveVariable(TypeVariable<?> variable) {
////            TypeVariable<?> variableToCompare = (TypeVariable)SerializableTypeWrapper.unwrap(variable);
////
////            for(int i = 0; i < this.variables.length; ++i) {
////                TypeVariable<?> resolvedVariable = (TypeVariable)SerializableTypeWrapper.unwrap(this.variables[i]);
////                if (ObjectUtils.nullSafeEquals(resolvedVariable, variableToCompare)) {
////                    return this.generics[i];
////                }
////            }
////
////            return null;
////        }
////
////        public Object getSource() {
////            return this.generics;
////        }
////    }
////
////    private static class DefaultVariableResolver implements ResolvableType.VariableResolver {
////        private final ResolvableType source;
////
////        DefaultVariableResolver(ResolvableType resolvableType) {
////            this.source = resolvableType;
////        }
////
////        @Nullable
////        public ResolvableType resolveVariable(TypeVariable<?> variable) {
////            return this.source.resolveVariable(variable);
////        }
////
////        public Object getSource() {
////            return this.source;
////        }
////    }
//
////    interface VariableResolver extends Serializable {
////        Object getSource();
////
////        @Nullable
////        ResolvableType resolveVariable(TypeVariable<?> variable);
////    }
//}
