package com.webank.eggroll.webapp.model;


import com.webank.eggroll.webapp.exception.CodeEnum;
import com.webank.eggroll.webapp.exception.ErrorCode;
import lombok.Data;

import java.util.Objects;

@Data
public class CommonResponse<T> {


    private int code;


    private String msg;


    private T data;


    private int count;

    public CommonResponse() {
    }

    public CommonResponse(int code, String msg) {
        this(code, msg, null);
    }

    public CommonResponse(int code, String msg, T data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    public static <T> CommonResponse<T> error() {
        return error(500, "SYS_ERROR");
    }

    public static <T> CommonResponse<T> error(String msg) {
        return error(500, msg);
    }

    public static <T> CommonResponse<T> error(T data, int code, String msg) {
        CommonResponse<T> response = new CommonResponse<>();
        response.code = code;
        response.msg = msg;
        response.data = data;
        return response;
    }

    public static <T> CommonResponse<T> error(int code, String msg) {
        return error(null, code, msg);
    }

    public static <T> CommonResponse<T> error(int code, String msg, T data) {
        CommonResponse<T> response = new CommonResponse<>();
        response.code = code;
        response.msg = msg;
        response.data = data;
        return response;
    }

    public static <T> CommonResponse<T> success() {
        return success(null);
    }

    public static <T> CommonResponse<T> success(T data) {

        return success(data, 0);
    }

    public static <T> CommonResponse<T> success(T data, Integer count) {
        CommonResponse<T> response = new CommonResponse<>();
        response.code = 0;
        response.msg = "success";
        response.data = data;
        response.count = count;
        return response;
    }

    public static <T> CommonResponse<T> success(T data, Long count) {
        CommonResponse<T> response = new CommonResponse<>();
        response.code = 0;
        response.msg = "success";
        response.data = data;
        response.count = Math.toIntExact(count);
        return response;
    }

    public static <T> CommonResponse<T> warn(String msg) {
        CommonResponse<T> response = new CommonResponse<>();
        response.code = ErrorCode.WARN;
        response.msg = msg;
        return response;
    }

    public static <T> CommonResponse<T> warn(String msg, T data) {
        CommonResponse<T> response = new CommonResponse<>();
        response.code = ErrorCode.WARN;
        response.msg = msg;
        response.data = data;
        return response;
    }

    public static <T> CommonResponse<T> success(T data, String msg) {
        CommonResponse<T> response = new CommonResponse<>();
        response.code = 0;
        response.msg = msg;
        response.data = data;
        return response;
    }


    public static <T> boolean ifFail(CommonResponse<T> response) {

        return Objects.isNull(response) || !CodeEnum.SUCCESS.getValue().equals(Integer.toString(response.getCode()));
    }

    public static <T> boolean ifSuccess(CommonResponse<T> response) {
        return CodeEnum.SUCCESS.getValue().equals(Integer.toString(response.getCode()));
    }
}