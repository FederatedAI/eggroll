package com.eggroll.core.pojo;

import java.util.Enumeration;
import java.util.Optional;

public enum JobProcessorTypes {
    DeepSpeed;

    public static String fromString(String color) {
        switch (color.toLowerCase()) {
            case "deepspeed":
                return DeepSpeed.name();
            default:
                throw new IllegalArgumentException("unsupported job processor type: " + color);
        }
    }
}
