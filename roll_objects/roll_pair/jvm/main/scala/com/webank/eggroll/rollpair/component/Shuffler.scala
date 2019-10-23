package com.webank.eggroll.rollpair.component

import java.util
import java.util.concurrent.{Callable, CompletableFuture, CountDownLatch, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.function.Supplier

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.datastructure.{Broker, LinkedBlockingBroker}
import com.webank.eggroll.core.error.DistributedRuntimeException
import com.webank.eggroll.core.meta.{ErPair, ErPairBatch, ErPartition, ErStore, Meta}
import com.webank.eggroll.core.meta.MetaModelPbSerdes._
import com.webank.eggroll.core.transfer.{GrpcTransferService, TransferClient}
import com.webank.eggroll.core.util.{Logging, ThreadPoolUtils}
import com.webank.eggroll.rollpair.io.RocksdbSortedKvAdapter

import scala.collection.immutable.Queue
import scala.collection.mutable

trait Shuffler {
  def start(): Unit
  def isFinished(): Boolean
  def waitUntilFinished(timeout: Long, unit: TimeUnit): Boolean
  def getTotalPartitionedCount(): Long
  def hasError(): Boolean
  def getError(): DistributedRuntimeException
}

object Shuffler  {
  val idPrefix = classOf[Shuffler].getSimpleName
  val idIndex = new AtomicInteger(0)
  def generateId(idPrefix: String = Shuffler.idPrefix,
                 idIndex: Int = Shuffler.idIndex.incrementAndGet()) = s"${idPrefix}-${idIndex}"
}

class DefaultShuffler(shuffleId: String,
                      broker: Broker[(Array[Byte], Array[Byte])],
                      outputStore: ErStore,
                      outputPartition: ErPartition,
                      partitionFunction: Array[Byte] => Int,
                      threadPoolSize: Int = 5) extends Shuffler with Logging {

  val partitionThreadPool = ThreadPoolUtils.newFixedThreadPool(threadPoolSize, s"${shuffleId}")
  val sendRecvThreadPool = ThreadPoolUtils.newFixedThreadPool(2, prefix = s"${shuffleId}-sendrecv")

  val outputPartitions = outputStore.partitions
  val outputPartitionCount = outputPartitions.length

  val partitionedBrokers = for (i <- 0 until outputPartitionCount) yield new LinkedBlockingBroker[(Array[Byte], Array[Byte])]()
  val partitionFinishLatch = new CountDownLatch(threadPoolSize)
  val shuffleFinishLatch = new CountDownLatch(3)

  val notFinishedPartitionThreadCount = new CountDownLatch(threadPoolSize)
  val isSendFinished = new AtomicBoolean(false)
  val isReceiveFinished = new AtomicBoolean(false)
  val totalPartitionedElementsCount = new AtomicLong(0L)
  val errors = new DistributedRuntimeException

  def start(): Unit = {
    GrpcTransferService.getOrCreateBroker(key = s"${shuffleId}-${outputPartition.id}", writeSignals = outputPartitionCount)

    for (i <- 0 until threadPoolSize) {
      val cf: CompletableFuture[Long] =
        CompletableFuture
          .supplyAsync(
            new Partitioner(broker, partitionedBrokers.toList, partitionFunction), partitionThreadPool)
          .whenCompleteAsync((result, exception) => {
            if (exception == null) {
              notFinishedPartitionThreadCount.countDown()

              if (notFinishedPartitionThreadCount.getCount <= 0) {
                partitionedBrokers.foreach(b => b.signalWriteFinished())
                shuffleFinishLatch.countDown()
                logInfo(s"finished partition for partition")
              }

              totalPartitionedElementsCount.addAndGet(result)
            } else {
              logError(s"error in computing partition ${i}", exception)
              errors.append(exception)
            }
          }, partitionThreadPool)
    }

    val sender: CompletableFuture[Long] =
      CompletableFuture.supplyAsync(new ShuffleSender(shuffleId = shuffleId,
          shuffledBrokers = partitionedBrokers.toList,
          outputPartitions = outputPartitions), sendRecvThreadPool)
      .whenCompleteAsync((result, exception) => {
        if (exception == null) {
          logInfo(s"finished send. total sent: ${result}")
          isSendFinished.compareAndSet(false, true)
          shuffleFinishLatch.countDown()
        } else {
          logError(s"error in send", exception)
          errors.append(exception)
        }
      }, sendRecvThreadPool)

    val receiver: CompletableFuture[Long] =
      CompletableFuture.supplyAsync(new ShuffleReceiver(shuffleId = shuffleId,
          outputPartition = outputPartition,
          totalPartitionsCount = outputPartitionCount), sendRecvThreadPool)
          .whenCompleteAsync((result, exception) => {
            if (exception == null) {
              isReceiveFinished.compareAndSet(false, true)
              shuffleFinishLatch.countDown()
            } else {
              logError(s"error in receive", exception)
              errors.append(exception)
            }
          }, sendRecvThreadPool)


    // notFinishedPartitionThreadCount.await()
    // partitionThreadPool.shutdown()
  }

  override def isFinished(): Boolean = notFinishedPartitionThreadCount.getCount() <= 0 && isSendFinished.get() && isReceiveFinished.get()

  override def waitUntilFinished(timeout: Long, unit: TimeUnit): Boolean = shuffleFinishLatch.await(timeout, unit)

  override def getTotalPartitionedCount(): Long = totalPartitionedElementsCount.get()

  override def hasError(): Boolean = errors.check()

  override def getError(): DistributedRuntimeException = errors

  class Partitioner(input: Broker[(Array[Byte], Array[Byte])],
                    output: List[Broker[(Array[Byte], Array[Byte])]],
                    partitionFunction: Array[Byte] => Int,
                   // todo: configurable
                    chunkSize: Int = 1000) extends Supplier[Long] {
    val partitionedElementsCount = new AtomicLong(0L)

    override def get(): Long = {
      val chunk = new util.ArrayList[(Array[Byte], Array[Byte])](chunkSize)
      while (!input.isClosable()) {
        chunk.clear()
        input.drainTo(chunk, chunkSize)

        chunk.forEach(t => {
          output(partitionFunction(t._1)).put(t)
          partitionedElementsCount.incrementAndGet()
        })
      }

      partitionedElementsCount.get()
    }
  }

  // todo: consider change (Array[Byte], Array[Byte]) to ErPair
  class ShuffleSender(shuffleId: String,
                      shuffledBrokers: List[Broker[(Array[Byte], Array[Byte])]],
                      outputPartitions: List[ErPartition],
                     // todo: make it configurable
                      chunkSize: Int = 100)
    extends Supplier[Long] {
    val notFinishedBrokerIndex = mutable.Set[Int]()
    val totalSent = new AtomicLong(0L)

    for (i <- 0 until shuffledBrokers.size) {
      notFinishedBrokerIndex.add(i)
    }

    override def get(): Long = {
      // todo: change to event-driven
      val transferClient = new TransferClient()

      val newlyFinishedIdx = mutable.ListBuffer[Int]()

      while (notFinishedBrokerIndex.nonEmpty) {
        notFinishedBrokerIndex.foreach(idx => {
          val sendBuffer = new util.ArrayList[(Array[Byte], Array[Byte])](chunkSize)
          val broker = shuffledBrokers(idx)
          var brokerIsClosable = false

          this.synchronized {
            if (broker.isWriteFinished() || broker.size() >= chunkSize) {
              broker.drainTo(sendBuffer, chunkSize)
              brokerIsClosable = broker.isClosable()
            }
          }

          if (!sendBuffer.isEmpty) {
            val pairs = mutable.ListBuffer[ErPair]()
            sendBuffer.forEach(pair => {
              pairs += ErPair(key = pair._1, value = pair._2)
            })

            val pairBatch = ErPairBatch(pairs = pairs.toList)

            var transferStatus = StringConstants.EMPTY
            if (brokerIsClosable) {
              transferStatus = StringConstants.TRANSFER_END
              newlyFinishedIdx += idx
            }

            transferClient.send(data = pairBatch.toProto.toByteArray,
              tag = s"${shuffleId}-${idx}",
              serverNode = outputPartitions(idx).node,
              status = if (brokerIsClosable) StringConstants.TRANSFER_END else StringConstants.EMPTY)
            totalSent.addAndGet(pairs.length)
          }
        })

        newlyFinishedIdx.foreach(idx => notFinishedBrokerIndex -= idx)
      }

      totalSent.get()
    }
  }

  class ShuffleReceiver(shuffleId: String,
                        outputPartition: ErPartition,
                        totalPartitionsCount: Int)
    extends Supplier[Long] {
    override def get(): Long = {
      val queue = GrpcTransferService.getOrCreateBroker(s"${shuffleId}-${outputPartition.id}")

      val path = EggPair.getDbPath(outputPartition)
      logInfo(s"outputPath: ${path}")
      val outputAdapter = new RocksdbSortedKvAdapter(path)

      while (!queue.isClosable()) {
        val binData = queue.poll(1, TimeUnit.SECONDS)

        if (binData != null) {
          val pbPairBatch = Meta.PairBatch.parseFrom(binData)

          val pairBatch = pbPairBatch.fromProto()

          val result = pairBatch.pairs.map(erPair => (erPair.key, erPair.value))
          logInfo(s"result: ${result}, length: ${result.length}")

          outputAdapter.writeBatch(pairBatch.pairs.map(erPair => (erPair.key, erPair.value)).iterator)
        }
      }

      outputAdapter.close()

      0L
    }
  }
}

