package com.eggroll.core.pojo;


public enum JobProcessorTypes {
    DeepSpeed("deepspeed"),
    FlowJob("flowjob");

    private final String name;
    JobProcessorTypes(String name){
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static String fromString(String color) {
        switch (color.toLowerCase()) {
            case "deepspeed":
                return DeepSpeed.getName();
            default:
                throw new IllegalArgumentException("unsupported job processor type: " + color);
        }
    }
}
