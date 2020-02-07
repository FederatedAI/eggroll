package com.webank.eggroll.rollsite.infra;

import com.google.common.base.Preconditions;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

// TODO:0: add method to clean the map
public class JobStatus {
    public static ConcurrentHashMap<String, String> jobIdToSessionId = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, CountDownLatch> jobIdToFinishLatch = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, String> tagkeyToObjType = new ConcurrentHashMap<>();

    private static final Object latchLock = new Object();

    public static boolean hasLatch(String jobId) {
        return jobIdToFinishLatch.containsKey(jobId);
    }

    public static void createLatch(String jobId, int latchCount) {
        if (!jobIdToFinishLatch.containsKey(jobId)) {
            synchronized (latchLock) {
                if (!jobIdToFinishLatch.containsKey(jobId)) {
                    Preconditions.checkArgument(latchCount > 0, "latch must > 0");
                    jobIdToFinishLatch.putIfAbsent(jobId, new CountDownLatch(latchCount));
                }
            }
        }
    }

    public static void countDownLatch(String jobId) {
        if (jobIdToFinishLatch.containsKey(jobId)) {
            jobIdToFinishLatch.get(jobId).countDown();
        } else {
            throw new IllegalStateException("jobId " + jobId + " does not exist");
        }
    }

    public static boolean isAllCountDown(String jobId) {
        if (jobIdToFinishLatch.containsKey(jobId)) {
            return jobIdToFinishLatch.get(jobId).getCount() <= 0L;
        } else {
            return false;
        }
    }

    public static void setType(String name, String type) {
        tagkeyToObjType.putIfAbsent(name, type);
    }

    public static String getType(String name) {
        return tagkeyToObjType.get(name);
    }
}
