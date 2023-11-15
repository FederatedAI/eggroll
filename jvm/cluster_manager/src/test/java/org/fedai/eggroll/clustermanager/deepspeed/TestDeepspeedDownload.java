package org.fedai.eggroll.clustermanager.deepspeed;

import com.webank.eggroll.core.meta.Containers;
import com.webank.eggroll.core.meta.DeepspeedDownload;
import com.webank.eggroll.core.meta.DsDownloadServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.grpc.ClusterManagerClient;
import org.fedai.eggroll.core.grpc.GrpcConnectionFactory;
import org.fedai.eggroll.core.pojo.ErEndpoint;
import org.fedai.eggroll.core.pojo.ErSessionMeta;
import org.fedai.eggroll.core.pojo.PrepareJobDownloadRequest;
import org.fedai.eggroll.core.pojo.PrepareJobDownloadResponse;
import org.fedai.eggroll.core.utils.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

//@SpringBootTest(classes = Application.class)
//@RunWith(SpringJUnit4ClassRunner.class)
public class TestDeepspeedDownload {

    Logger logger = LoggerFactory.getLogger(TestDeepspeedDownload.class);

    ErEndpoint endpoint = new ErEndpoint("localhost:4670");

//    case class PrepareJobDownloadRequest(sessionId: String, ranks: Array[Int],
//                                         compressMethod: String,
//                                         compressLevel: Int = 1,
//                                         contentType: ContentType.ContentType)


    @Test
    public void testDownload() {
        //new ErSessionMeta(id = "testing_reg"+System.currentTimeMillis()+"_"+scala.util.Random.nextInt(100).toString, options = Map(SessionConfKeys.CONFKEY_SESSION_PROCESSORS_PER_NODE -> "2"))
        ClusterManagerClient clusterManagerClient = new ClusterManagerClient(endpoint);
        PrepareJobDownloadRequest prepareJobDownloadRequest = new PrepareJobDownloadRequest();
        prepareJobDownloadRequest.setSessionId("deepspeed_session_20230705-175508-766715");
        PrepareJobDownloadResponse prepareJobDownloadResponse = clusterManagerClient.prepareJobDownload(new Context(), prepareJobDownloadRequest);
        String content = prepareJobDownloadResponse.getContent();

        System.err.println("content =========" + content);
//        message DsDownloadRequest {
//            string session_id = 1;
//            repeated int32 ranks = 2;
//            string compress_method = 3;
//            int32 compress_level = 4;
//            ContentType content_type = 5;
//        }


        Map contentMap = JsonUtil.json2Object(content, Map.class);
        contentMap.forEach((k, v) -> {
            ManagedChannel managedChannel = GrpcConnectionFactory.createManagedChannel(new ErEndpoint(k.toString()), true);
            DsDownloadServiceGrpc.DsDownloadServiceStub stub = DsDownloadServiceGrpc.newStub(managedChannel);
            List ranks = (List) ((List) v).stream().map((n) -> {
                return ((List) n).get(2);
            }).collect(Collectors.toList());
            System.err.println("xxxxxxxxxxxx" + ranks);
            DeepspeedDownload.DsDownloadRequest dsDownloadRequest = DeepspeedDownload.DsDownloadRequest.newBuilder().
                    setSessionId("deepspeed_session_20230705-175508-766715")
                    .addAllRanks(ranks).setCompressLevel(1).setCompressMethod("zip")
                    .setContentType(Containers.ContentType.ALL).build();
            stub.downloadBySplit(dsDownloadRequest, new StreamObserver<DeepspeedDownload.DsDownloadSplitResponse>() {
                @Override
                public void onNext(DeepspeedDownload.DsDownloadSplitResponse dsDownloadSplitResponse) {
                    System.err.println("onNext:" + dsDownloadSplitResponse);
                }

                @Override
                public void onError(Throwable throwable) {
                    System.err.println("onError:");
                    throwable.printStackTrace();
                }

                @Override
                public void onCompleted() {
                    System.err.println("onCompleted");
                }
            });
        });


        //  TransferServiceGrpc.TransferServiceStub.newStub()

//        JsonUtil.json2Object(content)
//
//        var result = clusterManagerClient.prepareJobDownload(job = PrepareJobDownloadRequest(sessionId = "deepspeed_session_20230705-175508-766715",ranks= new Array[Int](0),
//                compressMethod="",contentType =com.webank.eggroll.core.containers.meta.ContentType.ALL ))
//        var  prepareData :java.util.Map[String,String]=gson.fromJson(result.content,classOf[java.util.Map[String,String]])
//

//        getOrCreateSessionMeta.setId("testx_"+System.currentTimeMillis());
//        clusterManagerClient.getOrCreateSession(getOrCreateSessionMeta);
//        getOrCreateSessionMeta.setId("testx_"+System.currentTimeMillis());
//        clusterManagerClient.getOrCreateSession(getOrCreateSessionMeta);
//        getOrCreateSessionMeta.setId("testx_"+System.currentTimeMillis());
//        clusterManagerClient.getOrCreateSession(getOrCreateSessionMeta);
//        getOrCreateSessionMeta.setId("testx_"+System.currentTimeMillis());
//        clusterManagerClient.getOrCreateSession(getOrCreateSessionMeta);
        //     logger.info("====================>result.id = {} , result.status = {}" ,result.getId(),result.getStatus());
    }

    @Test
    public void testGetSession() {
        ErSessionMeta getOrCreateSessionMeta = new ErSessionMeta();
        getOrCreateSessionMeta.setId("testx_1692327334950");
        Map<String, String> options = new HashMap<>();
        options.put(Dict.CONFKEY_SESSION_PROCESSORS_PER_NODE, "2");
        getOrCreateSessionMeta.setOptions(options);
        getOrCreateSessionMeta.setActiveProcCount(1);
        getOrCreateSessionMeta.setTotalProcCount(4);
        ClusterManagerClient clusterManagerClient = new ClusterManagerClient(endpoint);
        ErSessionMeta result = clusterManagerClient.getSession(new Context(), getOrCreateSessionMeta);
        logger.info("====================>result.id = {} , result.status = {}", result.getId(), result.getStatus());
    }

    @Test
    public void testKillSession() {
        ErSessionMeta getOrCreateSessionMeta = new ErSessionMeta();
        getOrCreateSessionMeta.setId("testx_1692342357633");
        Map<String, String> options = new HashMap<>();
        options.put(Dict.CONFKEY_SESSION_PROCESSORS_PER_NODE, "2");
        getOrCreateSessionMeta.setOptions(options);
        getOrCreateSessionMeta.setActiveProcCount(1);
        getOrCreateSessionMeta.setTotalProcCount(4);
        ClusterManagerClient clusterManagerClient = new ClusterManagerClient(endpoint);
        ErSessionMeta result = clusterManagerClient.killSession(new Context(), getOrCreateSessionMeta);
        logger.info("====================>result.id = {} , result.status = {}", result.getId(), result.getStatus());
    }

    @Test
    public void testKillAllSession() {
        ErSessionMeta getOrCreateSessionMeta = new ErSessionMeta();
        getOrCreateSessionMeta.setId("testx_1692263572251");
        Map<String, String> options = new HashMap<>();
        options.put(Dict.CONFKEY_SESSION_PROCESSORS_PER_NODE, "2");
        getOrCreateSessionMeta.setOptions(options);
        getOrCreateSessionMeta.setActiveProcCount(1);
        getOrCreateSessionMeta.setTotalProcCount(4);
        ClusterManagerClient clusterManagerClient = new ClusterManagerClient(endpoint);
        ErSessionMeta result = clusterManagerClient.killAllSession(new Context(), getOrCreateSessionMeta);
        logger.info("====================>result.id = {} , result.status = {}", result.getId(), result.getStatus());
    }


    public static void main(String[] args) {

        System.err.println("===============");

        TestDeepspeedDownload testDeepspeedDownload = new TestDeepspeedDownload();
        testDeepspeedDownload.testDownload();
        //testUnaryCall();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
