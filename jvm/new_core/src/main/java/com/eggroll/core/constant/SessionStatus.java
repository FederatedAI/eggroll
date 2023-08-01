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


    PREPARE(false), NEW(false),NEW_TIMEOUT(true),ACTIVE(false),CLOSED(true),KILLED(true),ERROR(true),FINISHED(true);
    private SessionStatus(boolean isOver){
        this.isOver = isOver;
    }

    private  boolean isOver;


}
