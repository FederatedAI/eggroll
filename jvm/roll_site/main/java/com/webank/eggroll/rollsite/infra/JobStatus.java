package com.webank.eggroll.rollsite.infra;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AtomicLongMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
    private static final LoadingCache<String, Throwable> jobIdToError;
    private static final LoadingCache<String, AtomicLongMap<Integer>> jobIdToPutBatchFinishedCountPartitions;
    private static final LoadingCache<String, AtomicLongMap<Integer>> jobIdToPutBatchRequiredCountPartitions;
    private static final LoadingCache<String, ConcurrentSkipListSet<Integer>> jobIdToMarkedEndPartitions;

    private static final Object latchLock = new Object();
    private static final Object putBatchLock = new Object();
    private static final Object tagKeyLock = new Object();
    private static final Object markEndLock = new Object();
    private static final Object errorLock = new Object();

    static {
        jobIdToSessionId = CacheBuilder.newBuilder()
            .maximumSize(1000000)
            .concurrencyLevel(50)
            .expireAfterAccess(60, TimeUnit.HOURS)
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

        jobIdToPutBatchFinishedCountPartitions = CacheBuilder.newBuilder()
                .maximumSize(1000000)
                .concurrencyLevel(50)
                .expireAfterAccess(48, TimeUnit.HOURS)
                .recordStats()
                .build(new CacheLoader<String, AtomicLongMap<Integer>>() {
                    @Override
                    public AtomicLongMap<Integer> load(String key) throws Exception {
                        synchronized (putBatchLock) {
                            return AtomicLongMap.create();
                        }
                    }
                });

        jobIdToPutBatchRequiredCountPartitions = CacheBuilder.newBuilder()
                .maximumSize(1000000)
                .concurrencyLevel(50)
                .expireAfterAccess(48, TimeUnit.HOURS)
                .recordStats()
                .build(new CacheLoader<String, AtomicLongMap<Integer>>() {
                    @Override
                    public AtomicLongMap<Integer> load(String key) throws Exception {
                        synchronized (markEndLock) {
                            return AtomicLongMap.create();
                        }
                    }
                });

        jobIdToError = CacheBuilder.newBuilder()
                .maximumSize(1000000)
                .concurrencyLevel(50)
                .expireAfterAccess(48, TimeUnit.HOURS)
                .recordStats()
                .build(new CacheLoader<String, Throwable>() {
                    @Override
                    public Throwable load(String key) throws Exception {
                        throw new IllegalStateException("loading of this cache is not supported");
                    }
                });

        jobIdToMarkedEndPartitions = CacheBuilder.newBuilder()
            .maximumSize(1000000)
            .concurrencyLevel(50)
            .expireAfterAccess(48, TimeUnit.HOURS)
            .recordStats()
            .build(new CacheLoader<String, ConcurrentSkipListSet<Integer>>() {
                @Override
                public ConcurrentSkipListSet<Integer> load(String key) throws Exception {
                    throw new IllegalStateException("loading of this cache is not supported");
                }
            });
    }

    public static LoadingCache<String, String> getJobIdToSessionId() {
        return jobIdToSessionId;
    }

    public static LoadingCache<String, CountDownLatch> getJobIdToFinishLatch() {
        return jobIdToFinishLatch;
    }

    public static LoadingCache<String, AtomicLong> getJobIdToPutBatchRequiredCount() {
        return jobIdToPutBatchRequiredCount;
    }

    public static LoadingCache<String, AtomicLong> getJobIdToPutBatchFinishedCount() {
        return jobIdToPutBatchFinishedCount;
    }

    public static LoadingCache<String, String> getTagkeyToObjType() {
        return tagkeyToObjType;
    }

    public static void cleanupJobStatus(String jobId) {
        synchronized (latchLock) {
            synchronized (putBatchLock) {
                synchronized (tagKeyLock) {
                    synchronized (markEndLock) {
                        synchronized (errorLock) {
                            removeLatch(jobId);
                            removePutBatchRequiredCount(jobId);
                            removePutBatchFinishedCount(jobId);
                            removeJobIdToMarkEnd(jobId);
                            removePutBatchRequiredCountAllPartitions(jobId);
                            removePutBatchFinishedCountAllPartitions(jobId);
                            removeType(jobId);
                            removeJobError(jobId);
                        }
                    }
                }
            }
        }
    }

    public static boolean isJobIdToSessionRegistered(String jobId) {
        return jobIdToSessionId.getIfPresent(jobId) != null;
    }

    public static String putJobIdToSessionId(String jobId, String erSessionId) {
        String old = jobIdToSessionId.getIfPresent(jobId);
        jobIdToSessionId.put(jobId, erSessionId);
        return old;
    }

    private static void removeJobIdToSessionId(String jobId) {
        jobIdToSessionId.invalidate(jobId);
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

    private static void removeLatch(String jobId) {
        synchronized (latchLock) {
            jobIdToFinishLatch.invalidate(jobId);
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

    private static void removeType(String name) {
        synchronized (tagKeyLock) {
            tagkeyToObjType.invalidate(name);
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

    private static void removePutBatchRequiredCount(String jobId) {
        jobIdToPutBatchRequiredCount.invalidate(jobId);
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

    private static void removePutBatchFinishedCount(String jobId) {
        jobIdToPutBatchFinishedCount.invalidate(jobId);
    }

    public static boolean isPutBatchFinished(String jobId) {
        long requiredCount = getPutBatchRequiredCount(jobId);
        long finishedCount = getPutBatchFinishedCount(jobId);
        if (finishedCount > requiredCount) {
            throw new IllegalStateException("Illegal finishedCount: finishedCount is more than requiredCount");
        }

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

    public static boolean isPutBatchFinishedPerPartition(String jobId, int partitionId) {
        long requiredCount = getPutBatchRequiredCountPerPartition(jobId, partitionId);
        long finishedCount = getPutBatchFinishedCountPerPartition(jobId, partitionId);
        if (finishedCount > requiredCount) {
            throw new IllegalStateException("Illegal finishedCount: finishedCount is more than requiredCount");
        }

        return requiredCount == finishedCount && requiredCount > 0;
    }

    public static long increasePutBatchFinishedCountPerPartition(String jobId, int partitionId) {
        try {
            increasePutBatchFinishedCount(jobId);
            return jobIdToPutBatchFinishedCountPartitions.get(jobId).incrementAndGet(partitionId);
        }catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static long getPutBatchFinishedCountPerPartition(String jobId, int partitionId) {
        try {
            if(jobIdToPutBatchFinishedCountPartitions.get(jobId).containsKey(partitionId)) {
                return jobIdToPutBatchFinishedCountPartitions.get(jobId).get(partitionId);
            }
            else {
                return 0;
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static AtomicLongMap<Integer> getPutBatchFinishedCountAllPartitions(String jobId) {
        return jobIdToPutBatchFinishedCountPartitions.getIfPresent(jobId);
    }

    private static void removePutBatchFinishedCountAllPartitions(String jobId) {
        jobIdToPutBatchFinishedCountPartitions.invalidate(jobId);
    }

    public static boolean waitUntilPutBatchFinishedPerPartition(String jobId, long timeout, TimeUnit unit) {
        long timeoutWallClock = System.currentTimeMillis() + unit.toMillis(timeout);
        boolean result = false;
        long totalPartitons = 0;

        try {
            totalPartitons = jobIdToPutBatchRequiredCountPartitions.get(jobId).size();
        }catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        for(int partitionId = 0; partitionId < totalPartitons; partitionId = partitionId+1) {
            try {
                result = isPutBatchFinishedPerPartition(jobId, partitionId);
                while (!result && System.currentTimeMillis() <= timeoutWallClock) {
                    Thread.sleep(20);
                    result = isPutBatchFinishedPerPartition(jobId, partitionId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        return result;
    }

    public static long addPutBatchRequiredCountPerPartition(String jobId, int partitionId, long count) {
        synchronized (markEndLock) {
            AtomicLongMap<Integer> jobRequiredCountPerPartition = jobIdToPutBatchRequiredCountPartitions
                .getUnchecked(jobId);
            if (!jobRequiredCountPerPartition.containsKey(partitionId)) {
                jobRequiredCountPerPartition.addAndGet(partitionId, count);
                addPutBatchRequiredCount(jobId, count);
            }
            return jobRequiredCountPerPartition.get(partitionId);
        }
    }


    public static long getPutBatchRequiredCountPerPartition(String jobId, int partitionId) {
        try {
            return jobIdToPutBatchRequiredCountPartitions.get(jobId).get(partitionId);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static AtomicLongMap<Integer> getPutBatchRequiredCountAllPartitions(String jobId) {
        return jobIdToPutBatchRequiredCountPartitions.getIfPresent(jobId);
    }

    private static void removePutBatchRequiredCountAllPartitions(String jobId) {
        jobIdToPutBatchRequiredCountPartitions.invalidate(jobId);
    }

    public static Throwable addJobError(String jobId, Throwable jobError) {
        synchronized (errorLock) {
            Throwable old = jobIdToError.getIfPresent(jobId);
            jobIdToError.put(jobId, jobError);
            return old;
        }
    }

    public static Throwable getJobError(String jobId) {
        synchronized (errorLock) {
            return jobIdToError.getIfPresent(jobId);
        }
    }

    public static void removeJobError(String jobId) {
        synchronized (errorLock) {
            jobIdToError.invalidate(jobId);
        }
    }

    public static void createJobIdToMarkEnd(String jobId) {
        synchronized (markEndLock) {
            jobIdToMarkedEndPartitions.put(jobId, new ConcurrentSkipListSet<>());
        }
    }

    public static void removeJobIdToMarkEnd(String jobId) {
        synchronized (markEndLock) {
            jobIdToMarkedEndPartitions.invalidate(jobId);
        }
    }

    public static void setPartitionMarkedEnd(String jobId, Integer partitionId) {
        synchronized (markEndLock) {
            if (!hasPartitionMarkedEnd(jobId, partitionId)) {
                jobIdToMarkedEndPartitions.getUnchecked(jobId).add(partitionId);
                countDownFinishLatch(jobId);
            }
        }
    }

    public static boolean hasPartitionMarkedEnd(String jobId, Integer partitionId) {
        synchronized (markEndLock) {
            return jobIdToMarkedEndPartitions.getUnchecked(jobId).contains(partitionId);
        }
    }
}
