package com.eggroll.core.exceptions;

public class ErSessionException extends EggRollBaseException {

    public ErSessionException(String message) {
        super(message);
    }

    public ErSessionException(String exCode, String message) {
        super(exCode, message);
    }
}
