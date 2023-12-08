package org.fedai.eggroll.core.constant;



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

    private ResourceStatus(String value) {
        this.value = value;
    }

    private String value;

}
