package com.webank.eggroll.nodemanager.extend;

import com.eggroll.core.grpc.GrpcConnectionFactory;
import com.eggroll.core.pojo.ErEndpoint;
import com.webank.eggroll.core.transfer.Extend;
import com.webank.eggroll.core.transfer.ExtendTransferServerGrpc;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

public class NodeExtendTransferServiceTest {


    @Test
    public void test1() throws InterruptedException {

        ErEndpoint erEndpoint = new ErEndpoint("127.0.0.1", 4671);
        ManagedChannel managedChannel = GrpcConnectionFactory.createManagedChannel(erEndpoint,true);

        ExtendTransferServerGrpc.ExtendTransferServerStub stub = ExtendTransferServerGrpc.newStub(managedChannel);

        StreamObserver<Extend.GetLogRequest> requestSb = stub.getLog(new StreamObserver<Extend.GetLogResponse>() {

            @Override
            public void onNext(Extend.GetLogResponse getLogResponse) {
                System.err.println("receive log " + getLogResponse);
                System.out.println("here-----11-");
            }
            @Override
            public void onError(Throwable throwable) {
                System.err.println("receive log onError ");
                System.out.println("here------22");
                throwable.printStackTrace();
            }
            @Override
            public void onCompleted() {
                System.out.println("here------33");
                System.err.println("receive log onCompleted ");
            }
        });

        Extend.GetLogRequest request = Extend.GetLogRequest.newBuilder()
                .setSessionId("deepspeed_session_20230705-175508-766715")
                .setLogType("INFO")
                .setRank("2")
                .build();


        requestSb.onNext(request);
        try {
            Thread.sleep(5000);
        }catch (InterruptedException e) {
            e.printStackTrace();
        }

        requestSb.onCompleted();
    }
}
