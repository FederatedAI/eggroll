package org.fedai.eggroll.core.constant;


public enum SessionStatus {

    WAITING_RESOURCE("WAITING_RESOURCE",false),
    PREPARE("PREPARE", false),
    NEW("NEW", false),
    NEW_TIMEOUT("NEW_TIMEOUT", true),
    ACTIVE("ACTIVE", false),
    CLOSED("CLOSED", true),
    KILLED("KILLED", true),
    ERROR("ERROR", true),
    ALLOCATE_RESOURCE_FAILED("ALLOCATE_RESOURCE_FAILED",true),
    FINISHED("FINISHED", true);

    private SessionStatus(String name, boolean isOver) {
        this.name = name;
        this.isOver = isOver;
    }

    private boolean isOver;
    private String name;


}
