package com.webank.eggroll.core.exceptions;

public class ConfigErrorException extends EggRollBaseException{

    public ConfigErrorException(String message) {
        super(message);
    }

    public ConfigErrorException(int exCode, String message) {
        super(exCode, message);
    }
}
