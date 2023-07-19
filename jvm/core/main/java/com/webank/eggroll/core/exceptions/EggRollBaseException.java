package com.webank.eggroll.core.exceptions;

public class EggRollBaseException extends RuntimeException{
    private final Integer code;

    public EggRollBaseException(String message){
        super(message);
        this.code = 500;
    }

    public EggRollBaseException(int exCode, String message) {
        super(message);
        this.code = exCode;
    }

}
