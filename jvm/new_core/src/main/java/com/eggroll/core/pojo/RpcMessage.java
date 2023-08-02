package com.eggroll.core.pojo;


public interface RpcMessage {

      public   byte[]  serialize();

      public   void  deserialize(byte[]  data);

}