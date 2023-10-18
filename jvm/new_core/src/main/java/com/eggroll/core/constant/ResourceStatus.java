package com.eggroll.core.constant;


//  val INIT = "init"
//          val PRE_ALLOCATED = "pre_allocated"
//          val ALLOCATED = "allocated"
//          val ALLOCATE_FAILED = "allocate_failed"
//          val AVAILABLE = "available"
//          val RETURN = "return"


public enum ResourceStatus {
    INIT("init"),
    PRE_ALLOCATED("pre_allocated"),
    ALLOCATED("allocated"),
    ALLOCATE_FAILED("allocate_failed"),
    AVAILABLE("available"),
    RETURN("return");

    public String getValue() {
        return value;
    }

    private  ResourceStatus(String value){
        this.value = value;
    }
    private  String value;

}
