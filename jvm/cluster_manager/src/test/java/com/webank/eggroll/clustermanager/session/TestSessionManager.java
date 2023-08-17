package com.webank.eggroll.clustermanager.session;

import com.eggroll.core.config.Dict;
import com.eggroll.core.grpc.ClusterManagerClient;
import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErSessionMeta;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

//@SpringBootTest(classes = Application.class)
//@RunWith(SpringJUnit4ClassRunner.class)
public class TestSessionManager {


    ErEndpoint endpoint = new ErEndpoint("127.0.0.1:4670");

    @Test
    public void  testCreateSession(){
    }

    @Test
    public void testGetOrCreate() {
        //new ErSessionMeta(id = "testing_reg"+System.currentTimeMillis()+"_"+scala.util.Random.nextInt(100).toString, options = Map(SessionConfKeys.CONFKEY_SESSION_PROCESSORS_PER_NODE -> "2"))
        ErSessionMeta getOrCreateSessionMeta = new ErSessionMeta();
        getOrCreateSessionMeta.setId("testx_"+System.currentTimeMillis());
        Map<String,String> options = new HashMap<>();
        options.put(Dict.CONFKEY_SESSION_PROCESSORS_PER_NODE ,"2");
        getOrCreateSessionMeta.setOptions(options);
        getOrCreateSessionMeta.setName("Test1");
        ClusterManagerClient clusterManagerClient = new ClusterManagerClient(endpoint);
        ErSessionMeta result = clusterManagerClient.getOrCreateSession(getOrCreateSessionMeta);
        System.err.println(result);
    }

    @Test
    public void testGetSession() {
        ErSessionMeta getOrCreateSessionMeta = new ErSessionMeta();
        getOrCreateSessionMeta.setId("testx_1692263572251");
        Map<String,String> options = new HashMap<>();
        options.put(Dict.CONFKEY_SESSION_PROCESSORS_PER_NODE ,"2");
        getOrCreateSessionMeta.setOptions(options);
        ClusterManagerClient clusterManagerClient = new ClusterManagerClient(endpoint);
        ErSessionMeta result = clusterManagerClient.getSession(getOrCreateSessionMeta);
        System.out.println("testGetSession result = " + result);
    }

    @Test
    public void testKillSession() {
        ErSessionMeta getOrCreateSessionMeta = new ErSessionMeta();
        getOrCreateSessionMeta.setId("testx_1692263572251");
        Map<String,String> options = new HashMap<>();
        options.put(Dict.CONFKEY_SESSION_PROCESSORS_PER_NODE ,"2");
        getOrCreateSessionMeta.setOptions(options);
        ClusterManagerClient clusterManagerClient = new ClusterManagerClient(endpoint);
        ErSessionMeta result = clusterManagerClient.killSession(getOrCreateSessionMeta);
        System.out.println("testGetSession result = " + result);
    }

    @Test
    public void testKillAllSession() {
        ErSessionMeta getOrCreateSessionMeta = new ErSessionMeta();
        getOrCreateSessionMeta.setId("testx_1692263572251");
        Map<String,String> options = new HashMap<>();
        options.put(Dict.CONFKEY_SESSION_PROCESSORS_PER_NODE ,"2");
        getOrCreateSessionMeta.setOptions(options);
        ClusterManagerClient clusterManagerClient = new ClusterManagerClient(endpoint);
        ErSessionMeta result = clusterManagerClient.killAllSession();
        System.out.println("testGetSession result = " + result);
    }
}
