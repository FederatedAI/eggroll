package com.webank.eggroll.webapplication.exception;


public class ErrorCode {

    public static final int WARN = 300;
    //其他错误码

    //私有构造方法，防止类被实例化
    private ErrorCode() {
        throw new AssertionError();
    }
}
