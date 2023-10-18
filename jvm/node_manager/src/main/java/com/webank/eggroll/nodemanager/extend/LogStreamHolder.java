package com.webank.eggroll.nodemanager.extend;

import com.google.inject.Singleton;
import com.webank.eggroll.core.transfer.Extend;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


@Singleton
public class LogStreamHolder {
    Logger log = LoggerFactory.getLogger(LogStreamHolder.class);
    private long createTimestamp;
    private String command;
    private StreamObserver<Extend.GetLogResponse> streamObserver;
    private String status;

    private Thread thread;
    private BufferedReader bufferedReader;
    private Process process;

    public LogStreamHolder(long createTimestamp, String command, StreamObserver<Extend.GetLogResponse> streamObserver, String status) {
        this.createTimestamp = createTimestamp;
        this.command = command;
        this.streamObserver = streamObserver;
        this.status = status;
    }

    public void stop() {
        log.info("receive stop log stream command");
        this.status = "stop";
        thread.interrupt();
        process.destroyForcibly();

        try {
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        log.info("log begin to run");
        thread = new Thread(() -> {
            try {
                log.info("log stream begin {}", command);
                process = Runtime.getRuntime().exec(command);
                bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                int batchSize = 10;

                while (true) {
                    if (status.equals("stop")) {
                        break;
                    }

                    List<String> logBuffer = new ArrayList<>();
                    String line = bufferedReader.readLine();
                    if (line != null) {
                        logBuffer.add(line);
                    }

                    if (!logBuffer.isEmpty()) {
                        Extend.GetLogResponse response = Extend.GetLogResponse.newBuilder()
                                .setCode("0")
                                .addAllDatas(logBuffer)
                                .build();
                        streamObserver.onNext(response);
                    } else {
                        Thread.sleep(1000);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (bufferedReader != null) {
                        bufferedReader.close();
                    }

                    if (process != null) {
                        process.destroyForcibly();
                    }

                    if (streamObserver != null) {
                        try {
                            streamObserver.onCompleted();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    System.out.println("log stream destroy over");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        thread.start();
    }
}
