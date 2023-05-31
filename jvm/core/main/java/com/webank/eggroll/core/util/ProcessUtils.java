package com.webank.eggroll.core.util;

import com.sun.jna.Platform;

import java.io.*;

public class ProcessUtils {

    public static boolean checkProcess(String processId) {

        boolean flag = true;
        Process process = null;
        String command = "";


        try {
            if (Platform.isWindows()) {
                command ="cmd /c tasklist  /FI \"PID eq " + processId + "\"";
            } else if (Platform.isLinux() || Platform.isAIX()||Platform.isMac()) {
                command = "ps aux "
                        +
                        "| awk '{print $2}'"
                    +
                       "| grep -w  " + processId;
            }
            System.err.println(command);
            String[] cmd = new String[] { "/bin/sh", "-c", command };

             process = Runtime.getRuntime().exec(cmd);

            BufferedReader br = new BufferedReader(new InputStreamReader( process.getInputStream()));

            String line=null;
            StringBuffer b=new StringBuffer();
            while (true) {
                try {
                    if (!((line=br.readLine())!=null)) break;
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
                b.append(line+"\n");
            }
             return b.toString().contains(processId);
        } catch (IOException e) {
            //log.error(processId, e);
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
        return flag;
    }


    public  static  void main(String[] args){
       System.err.println( ProcessUtils.checkProcess("95609"));
    }

}
