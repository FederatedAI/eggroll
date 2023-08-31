package com.eggroll.core.postprocessor;


import lombok.Data;

@Data
public abstract class ApplicationStartedRunner {
    private Integer sequenceId = 0;
    public abstract void run(String[] args) throws Exception;
}
