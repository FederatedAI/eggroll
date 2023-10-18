package com.eggroll.core.constant;


//object SessionStatus {
//        val NEW = "NEW"
//        val NEW_TIMEOUT = "NEW_TIMEOUT"
//        val ACTIVE = "ACTIVE"
//        val CLOSED = "CLOSED"
//        val KILLED = "KILLED"
//        val ERROR = "ERROR"
//        val FINISHED = "FINISHED"
//        }

public enum SessionStatus {


    PREPARE("PREPARE", false), NEW("NEW", false), NEW_TIMEOUT("NEW_TIMEOUT", true), ACTIVE("ACTIVE", false), CLOSED("CLOSED", true), KILLED("KILLED", true), ERROR("ERROR", true), FINISHED("FINISHED", true);

    private SessionStatus(String name, boolean isOver) {
        this.name = name;
        this.isOver = isOver;
    }

    private boolean isOver;
    private String name;


}
