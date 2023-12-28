package org.fedai.eggroll.core.invoke;

import java.lang.reflect.Method;

public class InvokeInfo {
    public InvokeInfo(String uri, Object object, Method method, Class paramClass) {
        this.uri = uri;
        this.object = object;
        this.method = method;
        this.paramClass = paramClass;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public Method getMethod() {
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
    }

    public Class getParamClass() {
        return paramClass;
    }

    public void setParamClass(Class paramClass) {
        this.paramClass = paramClass;
    }

    String uri;
    Object object;
    Method method;
    Class paramClass;

    @Override
    public String toString() {
        return uri;
    }
}