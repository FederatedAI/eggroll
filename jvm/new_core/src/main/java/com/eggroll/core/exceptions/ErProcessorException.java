package com.eggroll.core.exceptions;

public class ErProcessorException extends EggRollBaseException{

    public ErProcessorException(String message) {
        super(message);
    }

    public ErProcessorException(int exCode, String message) {
        super(exCode, message);
    }
}
