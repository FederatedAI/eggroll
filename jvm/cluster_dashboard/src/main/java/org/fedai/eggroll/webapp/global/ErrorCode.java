package org.fedai.eggroll.webapp.global;




public enum ErrorCode {
    SUCCESS(0, "SUCCESS"),
    DATA_ERROR(10001,"Error occurs when getting data!"),// 一般获取不到数据或者数据库没有此数据才会报错
    NO_DATA(10002,"NO DATA"),
    SYS_ERROR(10010, "SYS_ERROR"),
    LOGIN_FAILED(10011, "USERNAME_OR_PASSWORD_ERROR"),
    PARAM_ERROR(10012, "PARAM_ERROR"),
    NO_LOGIN(10013, "NO_LOGIN"),
    // 其他的状态码可以写在这里
    ;
    public static final int WARN = 300;


    private int code;
    private String msg;

    ErrorCode(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public void setCode(int code) {
        this.code = code;

    }
}
