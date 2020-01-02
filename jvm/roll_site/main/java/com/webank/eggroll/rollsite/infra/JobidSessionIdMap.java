package com.webank.eggroll.rollsite.infra;

import java.util.concurrent.ConcurrentHashMap;

// TODO:0: add methord to clean the map
public class JobidSessionIdMap {
    public static ConcurrentHashMap<String, String> jobidSessionIdMap = new ConcurrentHashMap<>();
}
