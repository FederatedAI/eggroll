package com.webank.eggroll.clustermanager.register;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.utils.JsonUtil;
import com.eggroll.core.utils.NetUtils;
import com.google.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * author: ningdu
 * Date: 2023/8/3
 * Time: 14:22
 * Description:
 */


@Singleton
public class ZooKeeperRegistration {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperRegistration.class);
    private CuratorFramework client;

    // MetaInfo 获取
    private static final String PATH_SEPARATOR = "/";
    private final static String DEFAULT_COMPONENT_ROOT = "FATE-COMPONENTS";
    private static final String project = "eggroll";
    private static final int PORT = MetaInfo.ZOOKEEPER_PORT;
    private static final int timeout = 5000;
    private String TIMESTAMP_KEY = "timestamp";
    private static final String INSTANCE_ID = "instance_id";
    private static final String VERSION = "version";
    private boolean ENABLED = MetaInfo.ZOOKEEPER_ENABLED;


    public synchronized void register() {
        try {
            if (ENABLED) {
                String url = generateZkUrl(MetaInfo.ZOOKEEPER_HOST,MetaInfo.ZOOKEEPER_PORT);

                //String urls = MetaInfo.ZOOKEEPER_HOST+":"+2181;
                String localIp = NetUtils.getLocalIp();
                String path = PATH_SEPARATOR + DEFAULT_COMPONENT_ROOT + PATH_SEPARATOR + project + PATH_SEPARATOR + localIp + ":" + PORT;

                String instanceId = localIp + ":" + PORT;
                Map content = new HashMap();

                content.put(INSTANCE_ID, instanceId);
                content.put(TIMESTAMP_KEY, System.currentTimeMillis());
                content.put(VERSION, MetaInfo.ZOOKEEPER_VERSION);

                CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                        .connectString(url)
                        .retryPolicy(new RetryNTimes(1, 1000))
                        .connectionTimeoutMs(timeout);

                client = builder.build();
                client.start();
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(path, JsonUtil.object2Json(content).getBytes());
            }

            // 注册一个监听器，用于在进程退出时删除节点
//            Runtime.getRuntime().addShutdownHook(new Thread() {
//                public void run() {
//                    try {
//                        client.delete().deletingChildrenIfNeeded().forPath(path);
//                        client.close();
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            });

        } catch (Exception e) {
            logger.error("connect to zookeeper failed: {}", e.getMessage());
        }


    }

    public static String generateZkUrl(String hosts, int port) {
        if (StringUtils.isBlank(hosts)) {
            return null;
        }
        String[] split = hosts.split(",");
        StringBuilder sb = new StringBuilder(split[0]);
        sb.append(":" + port);
        return sb.toString();
    }



}
