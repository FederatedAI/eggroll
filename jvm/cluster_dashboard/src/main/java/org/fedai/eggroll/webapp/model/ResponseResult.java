package org.fedai.eggroll.webapp.model;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.fedai.eggroll.webapp.global.ErrorCode;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ResponseResult<T> {

    @JsonProperty(value = "code")
    private int code = 0;

    @JsonProperty(value = "msg")
    private String msg = "";
    @JsonProperty(value = "data")
    private T data;

    public ResponseResult(T data) {
        this.data = data;
    }

    public ResponseResult() {

    }

    public ResponseResult(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public ResponseResult(int code, T data) {
        this.code = code;
        this.data = data;
    }

    public ResponseResult(int code, String msg, T data) {
        this.code = code;
        this.data = data;
        this.msg = msg;
    }


    public ResponseResult(ErrorCode errorCode) {
        this.code = errorCode.getCode();
        this.msg = errorCode.getMsg();
    }

    public ResponseResult(ErrorCode errorCode, T data) {
        this.code = errorCode.getCode();
        this.msg = errorCode.getMsg();
        this.data = data;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public static <T> ResponseResult<T> success(T data) {
        ResponseResult<T> responseResult = new ResponseResult<>();
        responseResult.setData(data);
        responseResult.setCode(ErrorCode.SUCCESS.getCode());
        responseResult.setMsg(ErrorCode.SUCCESS.getMsg());
        return responseResult;
    }

    public static <T> ResponseResult<T> noData(ErrorCode errorCode) {
        ResponseResult<T> responseResult = new ResponseResult<>();
        responseResult.setCode(ErrorCode.SUCCESS.getCode());
        responseResult.setMsg(errorCode.getMsg());
        return responseResult;
    }

    public static <T> ResponseResult<T> error(int errorCode, String message) {
        ResponseResult<T> responseResult = new ResponseResult<>();
        responseResult.setCode(errorCode);
        responseResult.setMsg(message);
        return responseResult;
    }
}