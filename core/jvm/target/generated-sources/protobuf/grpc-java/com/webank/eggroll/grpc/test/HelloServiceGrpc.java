package com.webank.eggroll.grpc.test;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.22.2)",
    comments = "Source: grpc-test.proto")
public final class HelloServiceGrpc {

  private HelloServiceGrpc() {}

  public static final String SERVICE_NAME = "com.webank.eggroll.grpc.test.HelloService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest,
      com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> getUnaryCallMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "unaryCall",
      requestType = com.webank.eggroll.grpc.test.GrpcTest.HelloRequest.class,
      responseType = com.webank.eggroll.grpc.test.GrpcTest.HelloResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest,
      com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> getUnaryCallMethod() {
    io.grpc.MethodDescriptor<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest, com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> getUnaryCallMethod;
    if ((getUnaryCallMethod = HelloServiceGrpc.getUnaryCallMethod) == null) {
      synchronized (HelloServiceGrpc.class) {
        if ((getUnaryCallMethod = HelloServiceGrpc.getUnaryCallMethod) == null) {
          HelloServiceGrpc.getUnaryCallMethod = getUnaryCallMethod = 
              io.grpc.MethodDescriptor.<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest, com.webank.eggroll.grpc.test.GrpcTest.HelloResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "com.webank.eggroll.grpc.test.HelloService", "unaryCall"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.webank.eggroll.grpc.test.GrpcTest.HelloRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.webank.eggroll.grpc.test.GrpcTest.HelloResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new HelloServiceMethodDescriptorSupplier("unaryCall"))
                  .build();
          }
        }
     }
     return getUnaryCallMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest,
      com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> getPushMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "push",
      requestType = com.webank.eggroll.grpc.test.GrpcTest.HelloRequest.class,
      responseType = com.webank.eggroll.grpc.test.GrpcTest.HelloResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
  public static io.grpc.MethodDescriptor<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest,
      com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> getPushMethod() {
    io.grpc.MethodDescriptor<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest, com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> getPushMethod;
    if ((getPushMethod = HelloServiceGrpc.getPushMethod) == null) {
      synchronized (HelloServiceGrpc.class) {
        if ((getPushMethod = HelloServiceGrpc.getPushMethod) == null) {
          HelloServiceGrpc.getPushMethod = getPushMethod = 
              io.grpc.MethodDescriptor.<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest, com.webank.eggroll.grpc.test.GrpcTest.HelloResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
              .setFullMethodName(generateFullMethodName(
                  "com.webank.eggroll.grpc.test.HelloService", "push"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.webank.eggroll.grpc.test.GrpcTest.HelloRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.webank.eggroll.grpc.test.GrpcTest.HelloResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new HelloServiceMethodDescriptorSupplier("push"))
                  .build();
          }
        }
     }
     return getPushMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest,
      com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> getPullMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "pull",
      requestType = com.webank.eggroll.grpc.test.GrpcTest.HelloRequest.class,
      responseType = com.webank.eggroll.grpc.test.GrpcTest.HelloResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest,
      com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> getPullMethod() {
    io.grpc.MethodDescriptor<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest, com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> getPullMethod;
    if ((getPullMethod = HelloServiceGrpc.getPullMethod) == null) {
      synchronized (HelloServiceGrpc.class) {
        if ((getPullMethod = HelloServiceGrpc.getPullMethod) == null) {
          HelloServiceGrpc.getPullMethod = getPullMethod = 
              io.grpc.MethodDescriptor.<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest, com.webank.eggroll.grpc.test.GrpcTest.HelloResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "com.webank.eggroll.grpc.test.HelloService", "pull"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.webank.eggroll.grpc.test.GrpcTest.HelloRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.webank.eggroll.grpc.test.GrpcTest.HelloResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new HelloServiceMethodDescriptorSupplier("pull"))
                  .build();
          }
        }
     }
     return getPullMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static HelloServiceStub newStub(io.grpc.Channel channel) {
    return new HelloServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static HelloServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new HelloServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static HelloServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new HelloServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class HelloServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void unaryCall(com.webank.eggroll.grpc.test.GrpcTest.HelloRequest request,
        io.grpc.stub.StreamObserver<com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getUnaryCallMethod(), responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest> push(
        io.grpc.stub.StreamObserver<com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> responseObserver) {
      return asyncUnimplementedStreamingCall(getPushMethod(), responseObserver);
    }

    /**
     */
    public void pull(com.webank.eggroll.grpc.test.GrpcTest.HelloRequest request,
        io.grpc.stub.StreamObserver<com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getPullMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getUnaryCallMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.webank.eggroll.grpc.test.GrpcTest.HelloRequest,
                com.webank.eggroll.grpc.test.GrpcTest.HelloResponse>(
                  this, METHODID_UNARY_CALL)))
          .addMethod(
            getPushMethod(),
            asyncClientStreamingCall(
              new MethodHandlers<
                com.webank.eggroll.grpc.test.GrpcTest.HelloRequest,
                com.webank.eggroll.grpc.test.GrpcTest.HelloResponse>(
                  this, METHODID_PUSH)))
          .addMethod(
            getPullMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.webank.eggroll.grpc.test.GrpcTest.HelloRequest,
                com.webank.eggroll.grpc.test.GrpcTest.HelloResponse>(
                  this, METHODID_PULL)))
          .build();
    }
  }

  /**
   */
  public static final class HelloServiceStub extends io.grpc.stub.AbstractStub<HelloServiceStub> {
    private HelloServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private HelloServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected HelloServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new HelloServiceStub(channel, callOptions);
    }

    /**
     */
    public void unaryCall(com.webank.eggroll.grpc.test.GrpcTest.HelloRequest request,
        io.grpc.stub.StreamObserver<com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getUnaryCallMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.webank.eggroll.grpc.test.GrpcTest.HelloRequest> push(
        io.grpc.stub.StreamObserver<com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> responseObserver) {
      return asyncClientStreamingCall(
          getChannel().newCall(getPushMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public void pull(com.webank.eggroll.grpc.test.GrpcTest.HelloRequest request,
        io.grpc.stub.StreamObserver<com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getPullMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class HelloServiceBlockingStub extends io.grpc.stub.AbstractStub<HelloServiceBlockingStub> {
    private HelloServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private HelloServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected HelloServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new HelloServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.webank.eggroll.grpc.test.GrpcTest.HelloResponse unaryCall(com.webank.eggroll.grpc.test.GrpcTest.HelloRequest request) {
      return blockingUnaryCall(
          getChannel(), getUnaryCallMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.webank.eggroll.grpc.test.GrpcTest.HelloResponse pull(com.webank.eggroll.grpc.test.GrpcTest.HelloRequest request) {
      return blockingUnaryCall(
          getChannel(), getPullMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class HelloServiceFutureStub extends io.grpc.stub.AbstractStub<HelloServiceFutureStub> {
    private HelloServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private HelloServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected HelloServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new HelloServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> unaryCall(
        com.webank.eggroll.grpc.test.GrpcTest.HelloRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getUnaryCallMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.webank.eggroll.grpc.test.GrpcTest.HelloResponse> pull(
        com.webank.eggroll.grpc.test.GrpcTest.HelloRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getPullMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_UNARY_CALL = 0;
  private static final int METHODID_PULL = 1;
  private static final int METHODID_PUSH = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final HelloServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(HelloServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_UNARY_CALL:
          serviceImpl.unaryCall((com.webank.eggroll.grpc.test.GrpcTest.HelloRequest) request,
              (io.grpc.stub.StreamObserver<com.webank.eggroll.grpc.test.GrpcTest.HelloResponse>) responseObserver);
          break;
        case METHODID_PULL:
          serviceImpl.pull((com.webank.eggroll.grpc.test.GrpcTest.HelloRequest) request,
              (io.grpc.stub.StreamObserver<com.webank.eggroll.grpc.test.GrpcTest.HelloResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PUSH:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.push(
              (io.grpc.stub.StreamObserver<com.webank.eggroll.grpc.test.GrpcTest.HelloResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class HelloServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    HelloServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.webank.eggroll.grpc.test.GrpcTest.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("HelloService");
    }
  }

  private static final class HelloServiceFileDescriptorSupplier
      extends HelloServiceBaseDescriptorSupplier {
    HelloServiceFileDescriptorSupplier() {}
  }

  private static final class HelloServiceMethodDescriptorSupplier
      extends HelloServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    HelloServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (HelloServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new HelloServiceFileDescriptorSupplier())
              .addMethod(getUnaryCallMethod())
              .addMethod(getPushMethod())
              .addMethod(getPullMethod())
              .build();
        }
      }
    }
    return result;
  }
}
