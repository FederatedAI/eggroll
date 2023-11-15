package org.fedai.eggroll.core.exceptions;

public class PathNotExistException extends EggRollBaseException {
    public PathNotExistException(String message) {
        super(message);
    }

    public PathNotExistException(String exCode, String message) {
        super(exCode, message);
    }
}
