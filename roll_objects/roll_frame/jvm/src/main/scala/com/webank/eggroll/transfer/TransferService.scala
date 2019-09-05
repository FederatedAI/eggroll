/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webank.eggroll.transfer

import java.util.concurrent.{BlockingQueue, CountDownLatch, LinkedBlockingQueue}

import com.webank.eggroll.rollframe.{RollFrameGrpc, ServerNode, TransferServiceGrpc}
import com.webank.eggroll.util.ThrowableCollection
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver

import scala.collection.concurrent.TrieMap

case class BatchData(headerSize:Int, header:Array[Byte], bodySize:Int, body:Array[Byte])
case class BatchID(id: Array[Byte])

trait TransferService {

}


object GrpcTransferService {
  private val map = TrieMap[String, BlockingQueue[RollFrameGrpc.Batch]]()

  def getOrCreateQueue(key: String):BlockingQueue[RollFrameGrpc.Batch] = this.synchronized {
    if(!map.contains(key)) {
      map.put(key, new LinkedBlockingQueue[RollFrameGrpc.Batch]())
    }
    map(key)
  }
}

class GrpcTransferService() extends TransferServiceGrpc.TransferServiceImplBase{
  //  val finished:AtomicInteger = new AtomicInteger()
  //  var queue: mutable.Queue[RollFrameGrpc.Batch] = new mutable.Queue[RollFrameGrpc.Batch]
  override def write(responseObserver: StreamObserver[RollFrameGrpc.BatchID]): StreamObserver[RollFrameGrpc.Batch] = {
    //    finished.incrementAndGet()
    //    val inputStream = new GrpcInputStream(responseObserver)
    // TODO: queue id
    //    inputStream.queue = GrpcTransferService.getOrCreateQueue("job-1".getBytes())
    //    inputStream
    val queue = GrpcTransferService.getOrCreateQueue("job-1")
    var batchId:RollFrameGrpc.BatchID = null
    new StreamObserver[RollFrameGrpc.Batch] {
      override def onNext(value: RollFrameGrpc.Batch): Unit = {
        queue.put(value)
        batchId = value.getId
      }

      override def onError(t: Throwable): Unit = {
        t.printStackTrace()
        responseObserver.onError(t)
      }

      override def onCompleted(): Unit = {
        responseObserver.onNext(batchId)
        responseObserver.onCompleted()
      }
    }
  }
}
class CollectiveTransfer(nodes: List[ServerNode], timeout:Int = 600*1000) {
  private val clients = nodes.map{ node =>
    TransferServiceGrpc.newStub(
      ManagedChannelBuilder.forAddress(node.host, node.port).usePlaintext().build())
  }
//  Runtime.getRuntime.addShutdownHook(new Thread(){
//    override def run(): Unit = clients.foreach(_.wait())
//  })
  val queue = GrpcTransferService.getOrCreateQueue("job-1")
  val errors = new ThrowableCollection()
  var done = false
  def gather(batchIDs: List[RollFrameGrpc.BatchID]):Unit = {
    val countDownLatch = new CountDownLatch(nodes.size)

    // TODO: zip -> join
    clients.zip(batchIDs)foreach{ case(client, batchID) =>
      client.read(batchID, new StreamObserver[RollFrameGrpc.Batch] {
        override def onNext(value: RollFrameGrpc.Batch): Unit = queue.put(value)

        override def onError(t: Throwable): Unit = errors.append(t)

        override def onCompleted(): Unit = done = true
      })
    }
  }

  def push(nodeId:Int, batches: List[RollFrameGrpc.Batch]):Unit = {
    // TODO: route
    // TODO: reset
    val countDownLatch = new CountDownLatch(1)
    val reqs = clients(nodeId).write(new StreamObserver[RollFrameGrpc.BatchID] {
      override def onNext(value: RollFrameGrpc.BatchID): Unit = {
        println("pushed", value)
      }

      override def onError(t: Throwable): Unit = {
        t.printStackTrace()
        errors.append(t)
      }

      override def onCompleted(): Unit = {
        done = true
        countDownLatch.countDown()
      }
    })
    batches.foreach(reqs.onNext)
    errors.check()
    reqs.onCompleted()
    countDownLatch.await()
  }
  def scatter():Unit = {

  }
}