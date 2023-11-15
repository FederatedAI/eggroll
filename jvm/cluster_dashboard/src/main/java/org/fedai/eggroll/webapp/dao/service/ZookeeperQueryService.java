package org.fedai.eggroll.webapp.dao.service;

import com.google.inject.Singleton;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

@Singleton
public class ZookeeperQueryService {

    private CuratorFramework curatorFramework;
    //创建客户端
    public ZookeeperQueryService(String zkAddress) {
        this.curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zkAddress)
                .sessionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        this.curatorFramework.start();
    }

    //查询节点信息
    public String queryNodeInfo(String path) throws Exception {
        return new String(this.curatorFramework.getData().forPath(path));
    }
    //关闭连接
    public void close() {
        this.curatorFramework.close();
    }
}
