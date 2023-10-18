package com.eggroll.core.exceptions;

import lombok.Data;

public class RankNotExistException extends EggRollBaseException{
    public RankNotExistException(String message) {
        super(message);
    }

    public RankNotExistException(String exCode, String message) {
        super(exCode, message);
    }
}
