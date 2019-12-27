package com.webank.eggroll.rollsite.infra;

import java.util.concurrent.ConcurrentHashMap;

public class JobidSessionIdMap {
    public static ConcurrentHashMap<String, String> jobidSessionIdMap = new ConcurrentHashMap<>();
}
