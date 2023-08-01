package com.eggroll.core.exceptions;

public class CrudException extends EggRollBaseException{
    public CrudException(String message) {
        super(message);
    }

    public CrudException(int exCode, String message) {
        super(exCode, message);
    }
}
