package com.eggroll.core.postprocessor;


public interface ApplicationStartedRunner {

    default int getRunnerSequenceId(){
        return 0;
    }

    void run(String[] args) throws Exception;
}
