package org.fedai.eggroll.core.exceptions;

import lombok.Data;

@Data
public class EggRollBaseException extends RuntimeException {
    private String code;

    public EggRollBaseException(String message) {
        super(message);

    }

    public EggRollBaseException(String exCode, String message) {
        super(message);
        this.code = exCode;
    }

}
