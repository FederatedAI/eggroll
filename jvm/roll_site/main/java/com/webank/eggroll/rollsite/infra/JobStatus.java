package com.webank.eggroll.rollsite.infra;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// TODO:0: add method to clean the map
public class JobStatus {
    private static final LoadingCache<String, String> jobIdToSessionId;
    private static final LoadingCache<String, CountDownLatch> jobIdToFinishLatch;
    private static final LoadingCache<String, AtomicInteger> jobIdToPutBatchCount;
    private static final LoadingCache<String, String> tagkeyToObjType;


    static {
        jobIdToSessionId = CacheBuilder.newBuilder()
            .maximumSize(1000000)
            .expireAfterAccess(48, TimeUnit.HOURS)
            .recordStats()
            .softValues()
            .build(new CacheLoader<String, String>() {
                @Override
                public String load(String key) throws Exception {
                    throw new IllegalStateException("loading of this cache is not supported");
                }
            });

        jobIdToFinishLatch = CacheBuilder.newBuilder()
            .maximumSize(1000000)
            .expireAfterAccess(48, TimeUnit.HOURS)
            .recordStats()
            .removalListener(removalNotification -> {
                throw new IllegalStateException("removing " + removalNotification.getKey() + ", " + removalNotification.getValue() + ". reason: " + removalNotification.getCause());
            })
            .build(new CacheLoader<String, CountDownLatch>() {
                @Override
                public CountDownLatch load(String key) throws Exception {
                    throw new IllegalStateException("loading of this cache is not supported");
                }
            });

        jobIdToPutBatchCount = CacheBuilder.newBuilder()
            .maximumSize(1000000)
            .expireAfterAccess(48, TimeUnit.HOURS)
            .recordStats()
            .build(new CacheLoader<String, AtomicInteger>() {
                @Override
                public AtomicInteger load(String key) throws Exception {
                    synchronized (putBatchLock) {
                        return new AtomicInteger(0);
                    }
                }
            });

        tagkeyToObjType = CacheBuilder.newBuilder()
            .maximumSize(1000000)
            .expireAfterAccess(48, TimeUnit.HOURS)
            .recordStats()
            .build(new CacheLoader<String, String>() {
                @Override
                public String load(String key) throws Exception {
                    throw new IllegalStateException("loading of this cache is not supported");
                }
            });
    }

    private static final Object latchLock = new Object();
    private static final Object putBatchLock = new Object();

    public static boolean isJobIdToSessionRegistered(String jobId) {
        return jobIdToSessionId.getIfPresent(jobId) != null;
    }

    public static String putJobIdToSessionId(String jobId, String erSessionId) {
        String old = jobIdToSessionId.getIfPresent(jobId);
        jobIdToSessionId.put(jobId, erSessionId);
        return old;
    }

    public static String getErSessionId(String jobId) {
        return jobIdToSessionId.getIfPresent(jobId);
    }

    public static boolean hasLatch(String jobId) {
        synchronized (latchLock) {
            return jobIdToFinishLatch.getIfPresent(jobId) != null;
        }
    }

    public static void createLatch(String jobId, int latchCount) {
        try {
            synchronized (latchLock) {
                jobIdToFinishLatch.get(jobId, () -> {
                    Preconditions.checkArgument(latchCount > 0, "latch must > 0");
                    return new CountDownLatch(latchCount);
                });
            }
        } catch (ExecutionException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static void countDownFinishLatch(String jobId) {
        CountDownLatch latch = null;
        synchronized (latchLock) {
            latch = jobIdToFinishLatch.getIfPresent(jobId);
        }
        if (latch != null) {
            latch.countDown();
        } else {
            throw new IllegalStateException("jobId " + jobId + " does not exist");
        }
    }

    public static long getFinishLatchCount(String jobId) {
        CountDownLatch latch = null;
        synchronized (latchLock) {
            latch = jobIdToFinishLatch.getIfPresent(jobId);
        }
        if (latch != null) {
            return latch.getCount();
        } else {
            return Integer.MIN_VALUE;
        }
    }

    public static boolean isAllCountDown(String jobId) {
        CountDownLatch latch = null;
        synchronized (latchLock) {
            latch = jobIdToFinishLatch.getIfPresent(jobId);
        }
        if (latch != null) {
            return latch.getCount() <= 0L;
        } else {
            return false;
        }
    }

    public static void setType(String name, String type) {
        tagkeyToObjType.put(name, type);
    }

    public static String getType(String name) {
        return tagkeyToObjType.getIfPresent(name);
    }

    public static int increasePutBatchCount(String jobId) {
        try {
            synchronized (putBatchLock) {
                return jobIdToPutBatchCount.get(jobId).incrementAndGet();
            }
        } catch (ExecutionException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static int decreasePutBatchCount(String jobId) {
        AtomicInteger count = jobIdToPutBatchCount.getIfPresent(jobId);
        if (count != null) {
            return count.decrementAndGet();
        } else {
            return Integer.MIN_VALUE;
        }
    }

    public static int getPutBatchCount(String jobId) {
        AtomicInteger count = jobIdToPutBatchCount.getIfPresent(jobId);

        if (count != null) {
            return count.get();
        }

        return Integer.MIN_VALUE;
    }

    public static boolean isPutBatchFinished(String jobId) {
        return getPutBatchCount(jobId) == 0;
    }
}
