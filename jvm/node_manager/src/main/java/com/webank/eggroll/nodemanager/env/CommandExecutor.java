package com.webank.eggroll.nodemanager.env;

import java.io.IOException;

public interface CommandExecutor {

    void execute() throws IOException;

    int getExitCode() throws IOException;

    String getOutput() throws IOException;

    void close();

}