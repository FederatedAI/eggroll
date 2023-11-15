package com.eggroll.core.exceptions;

public class ConfigErrorException extends EggRollBaseException {

    public ConfigErrorException(String message) {
        super(message);
    }

    public ConfigErrorException(String exCode, String message) {
        super(exCode, message);
    }
}
