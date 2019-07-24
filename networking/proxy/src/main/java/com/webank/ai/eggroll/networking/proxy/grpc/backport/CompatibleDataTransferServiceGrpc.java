package com.webank.ai.eggroll.networking.proxy.grpc.backport;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.*;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.*;

/**
 * <pre>
 * data transfer service
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.16.1)",
    comments = "Source: proxy.proto")
public final class CompatibleDataTransferServiceGrpc {

  private CompatibleDataTransferServiceGrpc() {}

  public static final String SERVICE_NAME = "com.webank.ai.fate.api.networking.proxy.DataTransferService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet,
      com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata> getPushMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "push",
      requestType = com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet.class,
      responseType = com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata.class,
      methodType = io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
  public static io.grpc.MethodDescriptor<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet,
      com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata> getPushMethod() {
    io.grpc.MethodDescriptor<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet, com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata> getPushMethod;
    if ((getPushMethod = CompatibleDataTransferServiceGrpc.getPushMethod) == null) {
      synchronized (CompatibleDataTransferServiceGrpc.class) {
        if ((getPushMethod = CompatibleDataTransferServiceGrpc.getPushMethod) == null) {
          CompatibleDataTransferServiceGrpc.getPushMethod = getPushMethod =
              io.grpc.MethodDescriptor.<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet, com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
              .setFullMethodName(generateFullMethodName(
                  "com.webank.ai.fate.api.networking.proxy.DataTransferService", "push"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata.getDefaultInstance()))
                  .setSchemaDescriptor(new DataTransferServiceMethodDescriptorSupplier("push"))
                  .build();
          }
        }
     }
     return getPushMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata,
      com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> getPullMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "pull",
      requestType = com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata.class,
      responseType = com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata,
      com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> getPullMethod() {
    io.grpc.MethodDescriptor<com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata, com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> getPullMethod;
    if ((getPullMethod = CompatibleDataTransferServiceGrpc.getPullMethod) == null) {
      synchronized (CompatibleDataTransferServiceGrpc.class) {
        if ((getPullMethod = CompatibleDataTransferServiceGrpc.getPullMethod) == null) {
          CompatibleDataTransferServiceGrpc.getPullMethod = getPullMethod =
              io.grpc.MethodDescriptor.<com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata, com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(
                  "com.webank.ai.fate.api.networking.proxy.DataTransferService", "pull"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet.getDefaultInstance()))
                  .setSchemaDescriptor(new DataTransferServiceMethodDescriptorSupplier("pull"))
                  .build();
          }
        }
     }
     return getPullMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet,
      com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> getUnaryCallMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "unaryCall",
      requestType = com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet.class,
      responseType = com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet,
      com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> getUnaryCallMethod() {
    io.grpc.MethodDescriptor<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet, com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> getUnaryCallMethod;
    if ((getUnaryCallMethod = CompatibleDataTransferServiceGrpc.getUnaryCallMethod) == null) {
      synchronized (CompatibleDataTransferServiceGrpc.class) {
        if ((getUnaryCallMethod = CompatibleDataTransferServiceGrpc.getUnaryCallMethod) == null) {
          CompatibleDataTransferServiceGrpc.getUnaryCallMethod = getUnaryCallMethod =
              io.grpc.MethodDescriptor.<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet, com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "com.webank.ai.fate.api.networking.proxy.DataTransferService", "unaryCall"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet.getDefaultInstance()))
                  .setSchemaDescriptor(new DataTransferServiceMethodDescriptorSupplier("unaryCall"))
                  .build();
          }
        }
     }
     return getUnaryCallMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DataTransferServiceStub newStub(io.grpc.Channel channel) {
    return new DataTransferServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DataTransferServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new DataTransferServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DataTransferServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new DataTransferServiceFutureStub(channel);
  }

  /**
   * <pre>
   * data transfer service
   * </pre>
   */
  public static abstract class DataTransferServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public io.grpc.stub.StreamObserver<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> push(
        io.grpc.stub.StreamObserver<com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata> responseObserver) {
      return asyncUnimplementedStreamingCall(getPushMethod(), responseObserver);
    }

    /**
     */
    public void pull(com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata request,
        io.grpc.stub.StreamObserver<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> responseObserver) {
      asyncUnimplementedUnaryCall(getPullMethod(), responseObserver);
    }

    /**
     */
    public void unaryCall(com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet request,
        io.grpc.stub.StreamObserver<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> responseObserver) {
      asyncUnimplementedUnaryCall(getUnaryCallMethod(), responseObserver);
    }

    @Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getPushMethod(),
            asyncClientStreamingCall(
              new MethodHandlers<
                com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet,
                com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata>(
                  this, METHODID_PUSH)))
          .addMethod(
            getPullMethod(),
            asyncServerStreamingCall(
              new MethodHandlers<
                com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata,
                com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet>(
                  this, METHODID_PULL)))
          .addMethod(
            getUnaryCallMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet,
                com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet>(
                  this, METHODID_UNARY_CALL)))
          .build();
    }
  }

  /**
   * <pre>
   * data transfer service
   * </pre>
   */
  public static final class DataTransferServiceStub extends io.grpc.stub.AbstractStub<DataTransferServiceStub> {
    private DataTransferServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DataTransferServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected DataTransferServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DataTransferServiceStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> push(
        io.grpc.stub.StreamObserver<com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata> responseObserver) {
      return asyncClientStreamingCall(
          getChannel().newCall(getPushMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public void pull(com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata request,
        io.grpc.stub.StreamObserver<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(getPullMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void unaryCall(com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet request,
        io.grpc.stub.StreamObserver<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getUnaryCallMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * data transfer service
   * </pre>
   */
  public static final class DataTransferServiceBlockingStub extends io.grpc.stub.AbstractStub<DataTransferServiceBlockingStub> {
    private DataTransferServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DataTransferServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected DataTransferServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DataTransferServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> pull(
        com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata request) {
      return blockingServerStreamingCall(
          getChannel(), getPullMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet unaryCall(com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet request) {
      return blockingUnaryCall(
          getChannel(), getUnaryCallMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * data transfer service
   * </pre>
   */
  public static final class DataTransferServiceFutureStub extends io.grpc.stub.AbstractStub<DataTransferServiceFutureStub> {
    private DataTransferServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private DataTransferServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected DataTransferServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new DataTransferServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet> unaryCall(
        com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet request) {
      return futureUnaryCall(
          getChannel().newCall(getUnaryCallMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_PULL = 0;
  private static final int METHODID_UNARY_CALL = 1;
  private static final int METHODID_PUSH = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final DataTransferServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(DataTransferServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PULL:
          serviceImpl.pull((com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata) request,
              (io.grpc.stub.StreamObserver<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet>) responseObserver);
          break;
        case METHODID_UNARY_CALL:
          serviceImpl.unaryCall((com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet) request,
              (io.grpc.stub.StreamObserver<com.webank.ai.eggroll.api.networking.proxy.Proxy.Packet>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PUSH:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.push(
              (io.grpc.stub.StreamObserver<com.webank.ai.eggroll.api.networking.proxy.Proxy.Metadata>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class DataTransferServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DataTransferServiceBaseDescriptorSupplier() {}

    @Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.webank.ai.eggroll.api.networking.proxy.Proxy.getDescriptor();
    }

    @Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DataTransferService");
    }
  }

  private static final class DataTransferServiceFileDescriptorSupplier
      extends DataTransferServiceBaseDescriptorSupplier {
    DataTransferServiceFileDescriptorSupplier() {}
  }

  private static final class DataTransferServiceMethodDescriptorSupplier
      extends DataTransferServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    DataTransferServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (CompatibleDataTransferServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DataTransferServiceFileDescriptorSupplier())
              .addMethod(getPushMethod())
              .addMethod(getPullMethod())
              .addMethod(getUnaryCallMethod())
              .build();
        }
      }
    }
    return result;
  }
}
