package com.webank.eggroll.core.util;

import com.sun.jna.Platform;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

public class ProcessUtils {


    public  static int getCurrentPid(){
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            String name = runtime.getName(); // format: "pid@hostname"
            try {
                return Integer.parseInt(name.substring(0, name.indexOf('@')));
            } catch (Exception e) {
                return -1;
            }
    }


    public  static Process createProcess(String  command) throws IOException {
        String[] cmd = new String[] { "/bin/sh", "-c", command };
        return  Runtime.getRuntime().exec(cmd);
    }










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
            process = createProcess(command);
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
