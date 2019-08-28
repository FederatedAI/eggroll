package com.webank.ai.eggroll.rollsite.grpc.core.factory;

import com.google.protobuf.Message;
import com.webank.ai.eggroll.rollsite.grpc.core.api.grpc.client.GrpcAsyncClientContext;
import com.webank.ai.eggroll.rollsite.grpc.core.api.grpc.client.GrpcStreamingClientTemplate;
import io.grpc.stub.AbstractStub;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class GrpcCallModelFactory<S extends AbstractStub, R extends Message, E extends Message> {
    @Autowired
    private ApplicationContext applicationContext;

    public GrpcAsyncClientContext<S, R, E> createEndpointToEndpointContext(Class<? extends AbstractStub> stubClass) {
        GrpcAsyncClientContext<S, R, E> result =
                (GrpcAsyncClientContext<S, R, E>) applicationContext.getBean(GrpcAsyncClientContext.class);
        result.setStubClass(stubClass);

        return result;
    }

    public GrpcStreamingClientTemplate<S, R, E> createEndpointToEndpointTemplate() {
        return (GrpcStreamingClientTemplate<S, R, E>) applicationContext.getBean(GrpcStreamingClientTemplate.class);
    }
}
