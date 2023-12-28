package org.fedai.eggroll.nodemanager.utils;

import com.sun.jna.Platform;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ProcessUtils {


    public static Process createProcess(String command) throws IOException {
        String[] cmd = new String[]{"/bin/sh", "-c", command};
        return Runtime.getRuntime().exec(cmd);
    }


    public static boolean checkProcess(String processId) {

        boolean flag = true;
        Process process = null;
        String command = "";

        try {
            if (Platform.isWindows()) {
                command = "cmd /c tasklist  /FI \"PID eq " + processId + "\"";
            } else if (Platform.isLinux() || Platform.isAIX() || Platform.isMac()) {
                command = "ps aux "
                        +
                        "| awk '{print $2}'"
                        +
                        "| grep -w  " + processId;
            }
            process = createProcess(command);
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = null;
            StringBuffer b = new StringBuffer();
            while (true) {
                try {
                    if (!((line = br.readLine()) != null)) {
                        break;
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
                b.append(line + "\n");
            }
            return b.toString().contains(processId);
        } catch (IOException e) {
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
        return flag;
    }

}
