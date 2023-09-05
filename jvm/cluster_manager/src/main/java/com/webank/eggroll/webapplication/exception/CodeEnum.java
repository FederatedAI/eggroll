package com.webank.eggroll.webapplication.exception;


public enum CodeEnum {
    SUCCESS("0");
    // 其他状态码枚举值

    private final String value;

    private CodeEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }
}