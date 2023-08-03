package com.webank.eggroll.clustermanager.cluster;

import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.pojo.ErSessionMeta;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import org.checkerframework.checker.units.qual.K;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class ClusterResourceManager {

    Logger log = LoggerFactory.getLogger(ClusterResourceManager.class);

    Map<String, ReentrantLock> sessionLockMap = new ConcurrentHashMap<>();

    @Autowired
    SessionMainService sessionMainService;

    private Thread lockCleanThread = new Thread(() -> {
        while (true) {
            log.info("lock clean thread , prepare to run");
            long now = System.currentTimeMillis();
            sessionLockMap.forEach((k,v)->{
                try {
                    ErSessionMeta es = sessionMainService.getSessionMain(k);
                    if (es.getUpdateTime() != null) {
                        long updateTime = es.getUpdateTime().getTime();
                        if (now - updateTime > MetaInfo.EGGROLL_RESOURCE_LOCK_EXPIRE_INTERVAL
                                && (SessionStatus.KILLED == es.getStatus()
                                || es.status == SessionStatus.ERROR
                                || es.status == SessionStatus.CLOSED
                                || es.status == SessionStatus.FINISHED)) {
                            sessionLockMap.remove(es.getId());
                            killJobMap.remove(es.getId());
                        }
                    }
                } catch (Throwable e) {
                    logError("lock clean error: " + e.getMessage());
                    // e.printStackTrace();
                }
            });
            try {
                Thread.sleep(MetaInfo.EGGROLL_RESOURCE_LOCK_EXPIRE_INTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }, "LOCK-CLEAN-THREAD");

    public void lockSession(String sessionId){

    }
}
