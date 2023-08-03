package com.eggroll.core.grpc;

import com.eggroll.core.pojo.ErEndpoint;
import com.google.protobuf.ByteString;
import com.webank.eggroll.core.command.Command;
import com.webank.eggroll.core.command.CommandServiceGrpc;

public class CommandClient {


    public  byte[] call(ErEndpoint  erEndpoint,String uri, byte[] request){
        CommandServiceGrpc.CommandServiceBlockingStub stub = CommandServiceGrpc.newBlockingStub(GrpcConnectionFactory.createManagedChannel(erEndpoint,true));
        Command.CommandRequest.Builder  requestBuilder = Command.CommandRequest.newBuilder();
        requestBuilder.setId(System.currentTimeMillis() + "").setUri(uri).addArgs(ByteString.copyFrom(request));
        Command.CommandResponse  commandResponse = stub.call(requestBuilder.build());
        return commandResponse.getResults(0).toByteArray();
    }

    //  def call[T](commandUri: CommandURI, args: RpcMessage*)(implicit tag:ClassTag[T]): T = {
//    logDebug(s"[CommandClient.call, single endpoint] commandUri: ${commandUri.uriString}, endpoint: ${defaultEndpoint}")
//    try {
//      val stub = CommandServiceGrpc.newBlockingStub(GrpcClientUtils.getChannel(defaultEndpoint))
//      val argBytes = args.map(x => ByteString.copyFrom(SerdesUtils.rpcMessageToBytes(x, SerdesTypes.PROTOBUF)))
//      val resp = stub.call(Command.CommandRequest.newBuilder
//              .setId(System.currentTimeMillis + "")
//              .setUri(commandUri.uri.toString)
//              .addAllArgs(argBytes.asJava)
//              .build)
//      SerdesUtils.rpcMessageFromBytes(resp.getResults(0).toByteArray,
//              tag.runtimeClass, SerdesTypes.PROTOBUF).asInstanceOf[T]
//    } catch {
//      case t: Throwable =>
//        logError(s"[COMMAND] error calling to ${defaultEndpoint}, message: ${args(0)}. commandUri: ${commandUri.uriString}", t)
//        throw new CommandCallException(commandUri, defaultEndpoint, t)
//    }
//  }

}
