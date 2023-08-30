package com.eggroll.core.postprocessor;


import lombok.Data;

@Data
public abstract class ApplicationStartedListener {
    private Integer sequenceId = 0;
    public abstract void onApplicationStarted(String[] args) throws Exception;
}
