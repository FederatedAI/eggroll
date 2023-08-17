package com.eggroll.core.pojo;

import java.util.Enumeration;
import java.util.Optional;

public enum JobProcessorTypes {
    DeepSpeed;

    public static Optional<JobProcessorTypes> fromString(String type) {
        switch (type.toLowerCase()) {
            case "deepspeed":
                return Optional.of(DeepSpeed);
            default:
                return Optional.empty();
        }
    }

}