package com.eggroll.core.pojo;

import lombok.Data;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

@Data
public class FifoBroker<E> implements Iterator<E> {
    private Integer maxSize = 100;
    private Integer writers =1 ;
    private String name = "";
    private ArrayBlockingQueue<E> broker;
    private CountDownLatch remainingWriters;
    private ReentrantLock lock;

    public FifoBroker(){
        this.broker = new ArrayBlockingQueue<>(maxSize);
        this.remainingWriters = new CountDownLatch(writers);
        this.lock = new ReentrantLock();
    }

    public FifoBroker(int maxSize, int writers, String name) {
        this.maxSize = maxSize;
        this.writers = writers;
        this.name = name;
        this.broker = new ArrayBlockingQueue<>(maxSize);
        this.remainingWriters = new CountDownLatch(writers);
        this.lock = new ReentrantLock();
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if (!broker.isEmpty()) {
                return true;
            } else {
                if (getRemainingWritersCount() <= 0) {
                    return false;
                } else {
                    try {
                        Thread.sleep(10); // continue
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public E next() {
        try {
            return broker.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void signalWriteFinish() {
        if (remainingWriters.getCount() > 0) {
            remainingWriters.countDown();
        } else {
            throw new IllegalStateException(
                    "FifoBroker " + name + " countdown underflow." +
                            "maxSize=" + maxSize + ", " +
                            "writers=" + writers + ", " +
                            "remainingWriters=" + remainingWriters.getCount());
        }
    }

    public long getRemainingWritersCount() {
        return remainingWriters.getCount();
    }
}