package com.webank.eggroll.rollsite.infra;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

// TODO:0: add method to clean the map
public class JobStatus {
    private static final LoadingCache<String, String> jobIdToSessionId;
    private static final LoadingCache<String, CountDownLatch> jobIdToFinishLatch;
    private static final LoadingCache<String, AtomicLong> jobIdToPutBatchRequiredCount;
    private static final LoadingCache<String, AtomicLong> jobIdToPutBatchFinishedCount;
    private static final LoadingCache<String, String> tagkeyToObjType;

    static {
        jobIdToSessionId = CacheBuilder.newBuilder()
            .maximumSize(1000000)
            .concurrencyLevel(50)
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
            .concurrencyLevel(50)
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

        jobIdToPutBatchRequiredCount = CacheBuilder.newBuilder()
            .maximumSize(1000000)
            .concurrencyLevel(50)
            .expireAfterAccess(48, TimeUnit.HOURS)
            .recordStats()
            .build(new CacheLoader<String, AtomicLong>() {
                @Override
                public AtomicLong load(String key) throws Exception {
                    synchronized (putBatchLock) {
                        return new AtomicLong(0);
                    }
                }
            });

        jobIdToPutBatchFinishedCount = CacheBuilder.newBuilder()
            .maximumSize(1000000)
            .concurrencyLevel(50)
            .expireAfterAccess(48, TimeUnit.HOURS)
            .recordStats()
            .build(new CacheLoader<String, AtomicLong>() {
                @Override
                public AtomicLong load(String key) throws Exception {
                    synchronized (putBatchLock) {
                        return new AtomicLong(0);
                    }
                }
            });

        tagkeyToObjType = CacheBuilder.newBuilder()
            .maximumSize(1000000)
            .concurrencyLevel(50)
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
    private static final Object tagKeyLock = new Object();

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

    public static boolean waitUntilAllCountDown(String jobId, long timeout, TimeUnit unit) {
        CountDownLatch latch = null;
        long timeoutWallClock = System.currentTimeMillis() + unit.toMillis(timeout);
        try {
            while (latch == null && System.currentTimeMillis() <= timeoutWallClock) {
                synchronized (latchLock) {
                    latch = jobIdToFinishLatch.getIfPresent(jobId);
                }

                if (latch == null) {
                    Thread.sleep(50);
                }
            }

            if (latch != null) {
                return latch.await(timeout, unit);
            } else {
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

    }

    public static void setType(String name, String type) {
        if (tagkeyToObjType.getIfPresent(name) == null) {
            synchronized (tagKeyLock) {
                if (tagkeyToObjType.getIfPresent(name) == null) {
                    tagkeyToObjType.put(name, type);
                }
            }
        }
    }

    public static String getType(String name) {
        synchronized (tagKeyLock) {
            return tagkeyToObjType.getIfPresent(name);
        }
    }

    public static long addPutBatchRequiredCount(String jobId, long count) {
        try {
            return jobIdToPutBatchRequiredCount.get(jobId).addAndGet(count);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static long getPutBatchRequiredCount(String jobId) {
        try {
            return jobIdToPutBatchRequiredCount.get(jobId).get();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static long increasePutBatchFinishedCount(String jobId) {
        try {
            return jobIdToPutBatchFinishedCount.get(jobId).incrementAndGet();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static long getPutBatchFinishedCount(String jobId) {
        try {
            return jobIdToPutBatchFinishedCount.get(jobId).get();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isPutBatchFinished(String jobId) {
        long requiredCount = getPutBatchRequiredCount(jobId);
        long finishedCount = getPutBatchFinishedCount(jobId);
        return requiredCount == finishedCount && requiredCount > 0;
    }

    public static boolean waitUntilPutBatchFinished(String jobId, long timeout, TimeUnit unit) {
        long timeoutWallClock = System.currentTimeMillis() + unit.toMillis(timeout);
        boolean result = false;
        try {
            result = isPutBatchFinished(jobId);
            while (!result && System.currentTimeMillis() <= timeoutWallClock) {
                Thread.sleep(20);
                result = isPutBatchFinished(jobId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        return result;
    }
}
