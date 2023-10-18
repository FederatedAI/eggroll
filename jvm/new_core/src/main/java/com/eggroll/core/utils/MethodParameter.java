//
//package com.eggroll.core.utils;
//
//import java.lang.annotation.Annotation;
//import java.lang.reflect.AnnotatedElement;
//import java.lang.reflect.Constructor;
//import java.lang.reflect.Executable;
//import java.lang.reflect.Member;
//import java.lang.reflect.Method;
//import java.lang.reflect.Parameter;
//import java.lang.reflect.ParameterizedType;
//import java.lang.reflect.Type;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Optional;
//import java.util.function.Predicate;
//import kotlin.Unit;
//import kotlin.reflect.KFunction;
//import kotlin.reflect.KParameter;
//import kotlin.reflect.KParameter.Kind;
//import kotlin.reflect.jvm.ReflectJvmMapping;
//import org.springframework.lang.Nullable;
//import org.springframework.util.Assert;
//import org.springframework.util.ClassUtils;
//import org.springframework.util.ObjectUtils;
//
//public class MethodParameter {
//    private static final Annotation[] EMPTY_ANNOTATION_ARRAY = new Annotation[0];
//    private final Executable executable;
//    private final int parameterIndex;
//    @Nullable
//    private volatile Parameter parameter;
//    private int nestingLevel;
//    @Nullable
//    Map<Integer, Integer> typeIndexesPerLevel;
//    @Nullable
//    private volatile Class<?> containingClass;
//    @Nullable
//    private volatile Class<?> parameterType;
//    @Nullable
//    private volatile Type genericParameterType;
//    @Nullable
//    private volatile Annotation[] parameterAnnotations;
//    @Nullable
//    private volatile ParameterNameDiscoverer parameterNameDiscoverer;
//    @Nullable
//    private volatile String parameterName;
//    @Nullable
//    private volatile MethodParameter nestedMethodParameter;
//
//    public MethodParameter(Method method, int parameterIndex) {
//        this((Method)method, parameterIndex, 1);
//    }
//
//    public MethodParameter(Method method, int parameterIndex, int nestingLevel) {
//        Assert.notNull(method, "Method must not be null");
//        this.executable = method;
//        this.parameterIndex = validateIndex(method, parameterIndex);
//        this.nestingLevel = nestingLevel;
//    }
//
//    public MethodParameter(Constructor<?> constructor, int parameterIndex) {
//        this((Constructor)constructor, parameterIndex, 1);
//    }
//
//    public MethodParameter(Constructor<?> constructor, int parameterIndex, int nestingLevel) {
//        Assert.notNull(constructor, "Constructor must not be null");
//        this.executable = constructor;
//        this.parameterIndex = validateIndex(constructor, parameterIndex);
//        this.nestingLevel = nestingLevel;
//    }
//
//    MethodParameter(Executable executable, int parameterIndex, @Nullable Class<?> containingClass) {
//        Assert.notNull(executable, "Executable must not be null");
//        this.executable = executable;
//        this.parameterIndex = validateIndex(executable, parameterIndex);
//        this.nestingLevel = 1;
//        this.containingClass = containingClass;
//    }
//
//    public MethodParameter(MethodParameter original) {
//        Assert.notNull(original, "Original must not be null");
//        this.executable = original.executable;
//        this.parameterIndex = original.parameterIndex;
//        this.parameter = original.parameter;
//        this.nestingLevel = original.nestingLevel;
//        this.typeIndexesPerLevel = original.typeIndexesPerLevel;
//        this.containingClass = original.containingClass;
//        this.parameterType = original.parameterType;
//        this.genericParameterType = original.genericParameterType;
//        this.parameterAnnotations = original.parameterAnnotations;
//        this.parameterNameDiscoverer = original.parameterNameDiscoverer;
//        this.parameterName = original.parameterName;
//    }
//
//    @Nullable
//    public Method getMethod() {
//        return this.executable instanceof Method ? (Method)this.executable : null;
//    }
//
//    @Nullable
//    public Constructor<?> getConstructor() {
//        return this.executable instanceof Constructor ? (Constructor)this.executable : null;
//    }
//
//    public Class<?> getDeclaringClass() {
//        return this.executable.getDeclaringClass();
//    }
//
//    public Member getMember() {
//        return this.executable;
//    }
//
//    public AnnotatedElement getAnnotatedElement() {
//        return this.executable;
//    }
//
//    public Executable getExecutable() {
//        return this.executable;
//    }
//
//    public Parameter getParameter() {
//        if (this.parameterIndex < 0) {
//            throw new IllegalStateException("Cannot retrieve Parameter descriptor for method return type");
//        } else {
//            Parameter parameter = this.parameter;
//            if (parameter == null) {
//                parameter = this.getExecutable().getParameters()[this.parameterIndex];
//                this.parameter = parameter;
//            }
//
//            return parameter;
//        }
//    }
//
//    public int getParameterIndex() {
//        return this.parameterIndex;
//    }
//
//    /** @deprecated */
//    @Deprecated
//    public void increaseNestingLevel() {
//        ++this.nestingLevel;
//    }
//
//    /** @deprecated */
//    @Deprecated
//    public void decreaseNestingLevel() {
//        this.getTypeIndexesPerLevel().remove(this.nestingLevel);
//        --this.nestingLevel;
//    }
//
//    public int getNestingLevel() {
//        return this.nestingLevel;
//    }
//
//    public MethodParameter withTypeIndex(int typeIndex) {
//        return this.nested(this.nestingLevel, typeIndex);
//    }
//
//    /** @deprecated */
//    @Deprecated
//    public void setTypeIndexForCurrentLevel(int typeIndex) {
//        this.getTypeIndexesPerLevel().put(this.nestingLevel, typeIndex);
//    }
//
//    @Nullable
//    public Integer getTypeIndexForCurrentLevel() {
//        return this.getTypeIndexForLevel(this.nestingLevel);
//    }
//
//    @Nullable
//    public Integer getTypeIndexForLevel(int nestingLevel) {
//        return (Integer)this.getTypeIndexesPerLevel().get(nestingLevel);
//    }
//
//    private Map<Integer, Integer> getTypeIndexesPerLevel() {
//        if (this.typeIndexesPerLevel == null) {
//            this.typeIndexesPerLevel = new HashMap(4);
//        }
//
//        return this.typeIndexesPerLevel;
//    }
//
//    public MethodParameter nested() {
//        return this.nested((Integer)null);
//    }
//
//    public MethodParameter nested(@Nullable Integer typeIndex) {
//        MethodParameter nestedParam = this.nestedMethodParameter;
//        if (nestedParam != null && typeIndex == null) {
//            return nestedParam;
//        } else {
//            nestedParam = this.nested(this.nestingLevel + 1, typeIndex);
//            if (typeIndex == null) {
//                this.nestedMethodParameter = nestedParam;
//            }
//
//            return nestedParam;
//        }
//    }
//
//    private MethodParameter nested(int nestingLevel, @Nullable Integer typeIndex) {
//        MethodParameter copy = this.clone();
//        copy.nestingLevel = nestingLevel;
//        if (this.typeIndexesPerLevel != null) {
//            copy.typeIndexesPerLevel = new HashMap(this.typeIndexesPerLevel);
//        }
//
//        if (typeIndex != null) {
//            copy.getTypeIndexesPerLevel().put(copy.nestingLevel, typeIndex);
//        }
//
//        copy.parameterType = null;
//        copy.genericParameterType = null;
//        return copy;
//    }
//
//    public boolean isOptional() {
//        return this.getParameterType() == Optional.class || this.hasNullableAnnotation() || KotlinDetector.isKotlinReflectPresent() && KotlinDetector.isKotlinType(this.getContainingClass()) && MethodParameter.KotlinDelegate.isOptional(this);
//    }
//
//    private boolean hasNullableAnnotation() {
//        Annotation[] var1 = this.getParameterAnnotations();
//        int var2 = var1.length;
//
//        for(int var3 = 0; var3 < var2; ++var3) {
//            Annotation ann = var1[var3];
//            if ("Nullable".equals(ann.annotationType().getSimpleName())) {
//                return true;
//            }
//        }
//
//        return false;
//    }
//
//    public MethodParameter nestedIfOptional() {
//        return this.getParameterType() == Optional.class ? this.nested() : this;
//    }
//
//    public MethodParameter withContainingClass(@Nullable Class<?> containingClass) {
//        MethodParameter result = this.clone();
//        result.containingClass = containingClass;
//        result.parameterType = null;
//        return result;
//    }
//
//    /** @deprecated */
//    @Deprecated
//    void setContainingClass(Class<?> containingClass) {
//        this.containingClass = containingClass;
//        this.parameterType = null;
//    }
//
//    public Class<?> getContainingClass() {
//        Class<?> containingClass = this.containingClass;
//        return containingClass != null ? containingClass : this.getDeclaringClass();
//    }
//
//    /** @deprecated */
//    @Deprecated
//    void setParameterType(@Nullable Class<?> parameterType) {
//        this.parameterType = parameterType;
//    }
//
//    public Class<?> getParameterType() {
//        Class<?> paramType = this.parameterType;
//        if (paramType != null) {
//            return paramType;
//        } else {
//            if (this.getContainingClass() != this.getDeclaringClass()) {
//                paramType = ResolvableType.forMethodParameter(this, (Type)null, 1).resolve();
//            }
//
//            if (paramType == null) {
//                paramType = this.computeParameterType();
//            }
//
//            this.parameterType = paramType;
//            return paramType;
//        }
//    }
//
//    public Type getGenericParameterType() {
//        Type paramType = this.genericParameterType;
//        if (paramType == null) {
//            if (this.parameterIndex < 0) {
//                Method method = this.getMethod();
//                paramType = method != null ? (KotlinDetector.isKotlinReflectPresent() && KotlinDetector.isKotlinType(this.getContainingClass()) ? MethodParameter.KotlinDelegate.getGenericReturnType(method) : method.getGenericReturnType()) : Void.TYPE;
//            } else {
//                Type[] genericParameterTypes = this.executable.getGenericParameterTypes();
//                int index = this.parameterIndex;
//                if (this.executable instanceof Constructor && ClassUtils.isInnerClass(this.executable.getDeclaringClass()) && genericParameterTypes.length == this.executable.getParameterCount() - 1) {
//                    index = this.parameterIndex - 1;
//                }
//
//                paramType = index >= 0 && index < genericParameterTypes.length ? genericParameterTypes[index] : this.computeParameterType();
//            }
//
//            this.genericParameterType = (Type)paramType;
//        }
//
//        return (Type)paramType;
//    }
//
//    private Class<?> computeParameterType() {
//        if (this.parameterIndex < 0) {
//            Method method = this.getMethod();
//            if (method == null) {
//                return Void.TYPE;
//            } else {
//                return KotlinDetector.isKotlinReflectPresent() && KotlinDetector.isKotlinType(this.getContainingClass()) ? MethodParameter.KotlinDelegate.getReturnType(method) : method.getReturnType();
//            }
//        } else {
//            return this.executable.getParameterTypes()[this.parameterIndex];
//        }
//    }
//
//    public Class<?> getNestedParameterType() {
//        if (this.nestingLevel > 1) {
//            Type type = this.getGenericParameterType();
//
//            for(int i = 2; i <= this.nestingLevel; ++i) {
//                if (type instanceof ParameterizedType) {
//                    Type[] args = ((ParameterizedType)type).getActualTypeArguments();
//                    Integer index = this.getTypeIndexForLevel(i);
//                    type = args[index != null ? index : args.length - 1];
//                }
//            }
//
//            if (type instanceof Class) {
//                return (Class)type;
//            } else {
//                if (type instanceof ParameterizedType) {
//                    Type arg = ((ParameterizedType)type).getRawType();
//                    if (arg instanceof Class) {
//                        return (Class)arg;
//                    }
//                }
//
//                return Object.class;
//            }
//        } else {
//            return this.getParameterType();
//        }
//    }
//
//    public Type getNestedGenericParameterType() {
//        if (this.nestingLevel > 1) {
//            Type type = this.getGenericParameterType();
//
//            for(int i = 2; i <= this.nestingLevel; ++i) {
//                if (type instanceof ParameterizedType) {
//                    Type[] args = ((ParameterizedType)type).getActualTypeArguments();
//                    Integer index = this.getTypeIndexForLevel(i);
//                    type = args[index != null ? index : args.length - 1];
//                }
//            }
//
//            return type;
//        } else {
//            return this.getGenericParameterType();
//        }
//    }
//
//    public Annotation[] getMethodAnnotations() {
//        return this.adaptAnnotationArray(this.getAnnotatedElement().getAnnotations());
//    }
//
//    @Nullable
//    public <A extends Annotation> A getMethodAnnotation(Class<A> annotationType) {
//        A annotation = this.getAnnotatedElement().getAnnotation(annotationType);
//        return annotation != null ? this.adaptAnnotation(annotation) : null;
//    }
//
//    public <A extends Annotation> boolean hasMethodAnnotation(Class<A> annotationType) {
//        return this.getAnnotatedElement().isAnnotationPresent(annotationType);
//    }
//
//    public Annotation[] getParameterAnnotations() {
//        Annotation[] paramAnns = this.parameterAnnotations;
//        if (paramAnns == null) {
//            Annotation[][] annotationArray = this.executable.getParameterAnnotations();
//            int index = this.parameterIndex;
//            if (this.executable instanceof Constructor && ClassUtils.isInnerClass(this.executable.getDeclaringClass()) && annotationArray.length == this.executable.getParameterCount() - 1) {
//                index = this.parameterIndex - 1;
//            }
//
//            paramAnns = index >= 0 && index < annotationArray.length ? this.adaptAnnotationArray(annotationArray[index]) : EMPTY_ANNOTATION_ARRAY;
//            this.parameterAnnotations = paramAnns;
//        }
//
//        return paramAnns;
//    }
//
//    public boolean hasParameterAnnotations() {
//        return this.getParameterAnnotations().length != 0;
//    }
//
//    @Nullable
//    public <A extends Annotation> A getParameterAnnotation(Class<A> annotationType) {
//        Annotation[] anns = this.getParameterAnnotations();
//        Annotation[] var3 = anns;
//        int var4 = anns.length;
//
//        for(int var5 = 0; var5 < var4; ++var5) {
//            Annotation ann = var3[var5];
//            if (annotationType.isInstance(ann)) {
//                return ann;
//            }
//        }
//
//        return null;
//    }
//
//    public <A extends Annotation> boolean hasParameterAnnotation(Class<A> annotationType) {
//        return this.getParameterAnnotation(annotationType) != null;
//    }
//
//    public void initParameterNameDiscovery(@Nullable ParameterNameDiscoverer parameterNameDiscoverer) {
//        this.parameterNameDiscoverer = parameterNameDiscoverer;
//    }
//
//    @Nullable
//    public String getParameterName() {
//        if (this.parameterIndex < 0) {
//            return null;
//        } else {
//            ParameterNameDiscoverer discoverer = this.parameterNameDiscoverer;
//            if (discoverer != null) {
//                String[] parameterNames = null;
//                if (this.executable instanceof Method) {
//                    parameterNames = discoverer.getParameterNames((Method)this.executable);
//                } else if (this.executable instanceof Constructor) {
//                    parameterNames = discoverer.getParameterNames((Constructor)this.executable);
//                }
//
//                if (parameterNames != null) {
//                    this.parameterName = parameterNames[this.parameterIndex];
//                }
//
//                this.parameterNameDiscoverer = null;
//            }
//
//            return this.parameterName;
//        }
//    }
//
//    protected <A extends Annotation> A adaptAnnotation(A annotation) {
//        return annotation;
//    }
//
//    protected Annotation[] adaptAnnotationArray(Annotation[] annotations) {
//        return annotations;
//    }
//
//    public boolean equals(@Nullable Object other) {
//        if (this == other) {
//            return true;
//        } else if (!(other instanceof MethodParameter)) {
//            return false;
//        } else {
//            MethodParameter otherParam = (MethodParameter)other;
//            return this.getContainingClass() == otherParam.getContainingClass() && ObjectUtils.nullSafeEquals(this.typeIndexesPerLevel, otherParam.typeIndexesPerLevel) && this.nestingLevel == otherParam.nestingLevel && this.parameterIndex == otherParam.parameterIndex && this.executable.equals(otherParam.executable);
//        }
//    }
//
//    public int hashCode() {
//        return 31 * this.executable.hashCode() + this.parameterIndex;
//    }
//
//    public String toString() {
//        Method method = this.getMethod();
//        return (method != null ? "method '" + method.getName() + "'" : "constructor") + " parameter " + this.parameterIndex;
//    }
//
//    public MethodParameter clone() {
//        return new MethodParameter(this);
//    }
//
//    /** @deprecated */
//    @Deprecated
//    public static MethodParameter forMethodOrConstructor(Object methodOrConstructor, int parameterIndex) {
//        if (!(methodOrConstructor instanceof Executable)) {
//            throw new IllegalArgumentException("Given object [" + methodOrConstructor + "] is neither a Method nor a Constructor");
//        } else {
//            return forExecutable((Executable)methodOrConstructor, parameterIndex);
//        }
//    }
//
//    public static MethodParameter forExecutable(Executable executable, int parameterIndex) {
//        if (executable instanceof Method) {
//            return new MethodParameter((Method)executable, parameterIndex);
//        } else if (executable instanceof Constructor) {
//            return new MethodParameter((Constructor)executable, parameterIndex);
//        } else {
//            throw new IllegalArgumentException("Not a Method/Constructor: " + executable);
//        }
//    }
//
//    public static MethodParameter forParameter(Parameter parameter) {
//        return forExecutable(parameter.getDeclaringExecutable(), findParameterIndex(parameter));
//    }
//
//    protected static int findParameterIndex(Parameter parameter) {
//        Executable executable = parameter.getDeclaringExecutable();
//        Parameter[] allParams = executable.getParameters();
//
//        int i;
//        for(i = 0; i < allParams.length; ++i) {
//            if (parameter == allParams[i]) {
//                return i;
//            }
//        }
//
//        for(i = 0; i < allParams.length; ++i) {
//            if (parameter.equals(allParams[i])) {
//                return i;
//            }
//        }
//
//        throw new IllegalArgumentException("Given parameter [" + parameter + "] does not match any parameter in the declaring executable");
//    }
//
//    private static int validateIndex(Executable executable, int parameterIndex) {
//        int count = executable.getParameterCount();
//        Assert.isTrue(parameterIndex >= -1 && parameterIndex < count, () -> {
//            return "Parameter index needs to be between -1 and " + (count - 1);
//        });
//        return parameterIndex;
//    }
//
//    private static class KotlinDelegate {
//        private KotlinDelegate() {
//        }
//
//        public static boolean isOptional(MethodParameter param) {
//            Method method = param.getMethod();
//            int index = param.getParameterIndex();
//            KFunction function;
//            if (method != null && index == -1) {
//                function = ReflectJvmMapping.getKotlinFunction(method);
//                return function != null && function.getReturnType().isMarkedNullable();
//            } else {
//                Predicate predicate;
//                if (method != null) {
//                    if (param.getParameterType().getName().equals("kotlin.coroutines.Continuation")) {
//                        return true;
//                    }
//
//                    function = ReflectJvmMapping.getKotlinFunction(method);
//                    predicate = (p) -> {
//                        return Kind.VALUE.equals(p.getKind());
//                    };
//                } else {
//                    Constructor<?> ctor = param.getConstructor();
//                    Assert.state(ctor != null, "Neither method nor constructor found");
//                    function = ReflectJvmMapping.getKotlinFunction(ctor);
//                    predicate = (p) -> {
//                        return Kind.VALUE.equals(p.getKind()) || Kind.INSTANCE.equals(p.getKind());
//                    };
//                }
//
//                if (function != null) {
//                    int i = 0;
//                    Iterator var6 = function.getParameters().iterator();
//
//                    while(var6.hasNext()) {
//                        KParameter kParameter = (KParameter)var6.next();
//                        if (predicate.test(kParameter) && index == i++) {
//                            return kParameter.getType().isMarkedNullable() || kParameter.isOptional();
//                        }
//                    }
//                }
//
//                return false;
//            }
//        }
//
//        private static Type getGenericReturnType(Method method) {
//            try {
//                KFunction<?> function = ReflectJvmMapping.getKotlinFunction(method);
//                if (function != null && function.isSuspend()) {
//                    return ReflectJvmMapping.getJavaType(function.getReturnType());
//                }
//            } catch (UnsupportedOperationException var2) {
//            }
//
//            return method.getGenericReturnType();
//        }
//
//        private static Class<?> getReturnType(Method method) {
//            try {
//                KFunction<?> function = ReflectJvmMapping.getKotlinFunction(method);
//                if (function != null && function.isSuspend()) {
//                    Type paramType = ReflectJvmMapping.getJavaType(function.getReturnType());
//                    if (paramType == Unit.class) {
//                        paramType = Void.TYPE;
//                    }
//
//                    return ResolvableType.forType((Type)paramType).resolve(method.getReturnType());
//                }
//            } catch (UnsupportedOperationException var3) {
//            }
//
//            return method.getReturnType();
//        }
//    }
//}
