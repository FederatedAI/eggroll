package com.webank.eggroll.core.exceptions;

public class CrudException_JAVA extends EggRollBaseException{
    public CrudException_JAVA(String message) {
        super(message);
    }

    public CrudException_JAVA(int exCode, String message) {
        super(exCode, message);
    }
}
