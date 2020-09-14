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
 *
 *
 */

package com.webank.eggroll.format

import java.io._
import java.net.ConnectException
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.io.util.IoUtils
import com.webank.eggroll.core.meta.{ErPartition, ErStore, ErStoreLocator}
import com.webank.eggroll.rollframe.{HttpUtil, NioTransferEndpoint}
import io.netty.util.internal.PlatformDependent
import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.message._
import org.apache.arrow.vector.ipc.{ArrowReader, ArrowStreamReader, ArrowStreamWriter, ReadChannel}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{BitVectorHelper, FieldVector, Float8Vector, VectorSchemaRoot, VectorUnloader}
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.{Set => mutableSet}

// TODO: where to delete a RollFrame?
trait FrameStore {
  def close(): Unit

  def readAll(): Iterator[FrameBatch]

  def writeAll(batches: Iterator[FrameBatch]): Unit

  def append(batch: FrameBatch): Unit = writeAll(Iterator(batch))

  def readOne(): FrameBatch = readAll().next() // need to check iterator whether hasNext element
}

object FrameStore {
  val FILE: String = StringConstants.FILE
  val CACHE: String = StringConstants.CACHE
  val HDFS: String = StringConstants.HDFS
  val NETWORK: String = StringConstants.NETWORK
  val QUEUE: String = StringConstants.QUEUE
  val TOTAL: String = StringConstants.TOTAL

  val PATH: String = StringConstants.PATH
  val TYPE: String = StringConstants.TYPE
  val HOST: String = StringConstants.HOST
  val PORT: String = StringConstants.PORT

  val NETWORK_CONNECT_NUM = 3
  private val ROOT_PATH = "/tmp/unittests/RollFrameTests/"

  def getPartitionsMeta(store: ErStore): Seq[Map[String, String]] = {
    val storeDir = getStoreDir(store)
    store.partitions.indices.map(i => Map("path" -> s"$storeDir$i",
      "host" -> store.partitions(i).processor.transferEndpoint.host,
      "port" -> store.partitions(i).processor.transferEndpoint.port.toString))
  }

  def getStoreDir(store: ErStore): String = {
    val path = store.storeLocator.path
    if (StringUtils.isBlank(path))
      s"$ROOT_PATH/${store.storeLocator.toPath()}/"
    else
      path
  }

  def getStoreDir(erStoreLocator: ErStoreLocator): String = {
    val path = erStoreLocator.path
    if (StringUtils.isBlank(path))
      s"$ROOT_PATH/${erStoreLocator.toPath()}/"
    else
      path
  }

  def getStoreDir(namespace: String, name: String, storeType: String = StringConstants.CACHE): String = {
    s"$ROOT_PATH/${String.join(StringConstants.SLASH, storeType, namespace, name)}/"
  }

  def getStorePath(store: ErStore, partitionId: Int): String = {
    s"${getStoreDir(store)}$partitionId"
  }

  def getStorePath(partition: ErPartition): String = {
    IoUtils.getPath(partition, ROOT_PATH)
  }

  def apply(opts: Map[String, String]): FrameStore = opts.getOrElse(TYPE, FILE) match {
    case CACHE => new JvmFrameStore(opts(PATH))
    case QUEUE => new QueueFrameStore(opts(PATH), opts(TOTAL).toInt)
    case HDFS => new HdfsFrameStore(opts(PATH))
    case NETWORK => new NetworkFrameStore(opts(PATH), opts(HOST), opts(PORT).toInt) // TODO: add total num
    case _ => new FileFrameStore(opts(PATH))
  }

  /**
   * if want to support network FrameDB, it must has process address,so ErStore must contain full partition message.
   *
   * @param store       : FrameBatch store
   * @param partitionId : 0,1,2 ...
   * @return
   */
  def apply(store: ErStore, partitionId: Int): FrameStore =
    apply(Map(PATH -> getStorePath(store, partitionId), TYPE -> store.storeLocator.storeType,
      HOST -> store.partitions(partitionId).processor.transferEndpoint.host,
      PORT -> store.partitions(partitionId).processor.transferEndpoint.port.toString))

  def apply(partition: ErPartition): FrameStore = {
    apply(Map(PATH -> getStorePath(partition), TYPE -> partition.storeLocator.storeType,
      HOST -> partition.processor.transferEndpoint.host, PORT -> partition.processor.transferEndpoint.port.toString))
  }

  def queue(path: String, total: Int): FrameStore = apply(Map(PATH -> path, TOTAL -> total.toString, TYPE -> QUEUE))

  def file(path: String): FrameStore = apply(Map(PATH -> path, TYPE -> FILE))

  def cache(path: String): FrameStore = apply(Map(PATH -> path, TYPE -> CACHE))

  def hdfs(path: String): FrameStore = apply(Map(PATH -> path, TYPE -> HDFS))

  def network(path: String, host: String, port: String): FrameStore = apply(Map(PATH -> path, TYPE -> NETWORK,
    HOST -> host, PORT -> port))
}

// NOT thread safe
class FileFrameStore(path: String) extends FrameStore {
  var frameReader: FrameReader = _
  var frameWriter: FrameWriter = _

  override def close(): Unit = {
    if (frameReader != null) frameReader.close()
    if (frameWriter != null) frameWriter.close()
  }

  override def readAll(): Iterator[FrameBatch] = {
    frameReader = new FrameReader(new FileBlockAdapter(path))
    frameReader.getColumnarBatches()
  }

  def readAll(nullableFields: Set[Int] = Set[Int]()): Iterator[FrameBatch] = {
    val dir = new File(path).getParentFile
    if (!dir.exists()) dir.mkdirs()
    frameReader = new FrameReader(new FileBlockAdapter(path))
    frameReader.nullableFields = nullableFields
    frameReader.getColumnarBatches()
  }

  override def writeAll(batches: Iterator[FrameBatch]): Unit = {
    batches.foreach { batch =>
      if (frameWriter == null) {
        frameWriter = new FrameWriter(batch, new FileBlockAdapter(path))
        frameWriter.write()
      } else {
        frameWriter.writeSibling(batch)
      }
    }
  }
}

class HdfsFrameStore(path: String) extends FrameStore {
  var frameReader: FrameReader = _
  var frameWriter: FrameWriter = _

  def close(): Unit = {
    if (frameReader != null) frameReader.close()
    if (frameWriter != null) frameWriter.close()
  }

  def readAll(): Iterator[FrameBatch] = {
    frameReader = new FrameReader(new HdfsBlockAdapter(path))
    frameReader.getColumnarBatches()
  }

  def readAll(nullableFields: Set[Int] = Set[Int]()): Iterator[FrameBatch] = {
    frameReader = new FrameReader(new HdfsBlockAdapter(path))
    frameReader.nullableFields = nullableFields
    frameReader.getColumnarBatches()
  }

  def writeAll(batches: Iterator[FrameBatch]): Unit = {
    batches.foreach { batch =>
      if (frameWriter == null) {
        frameWriter = new FrameWriter(batch, new HdfsBlockAdapter(path))
        frameWriter.write()
      } else {
        frameWriter.writeSibling(batch)
      }
    }
  }
}

object QueueFrameStore {
  val map = TrieMap[String, BlockingQueue[FrameBatch]]()

  // key: a task name,e.g. mapBatch-0-doing , BlockingQueue[]: several batch FrameBatch
  def getOrCreateQueue(key: String): BlockingQueue[FrameBatch] = this.synchronized {
    if (!map.contains(key)) {
      map.put(key, new LinkedBlockingQueue[FrameBatch]())
    }
    map(key)
  }
}

class QueueFrameStore(path: String, total: Int) extends FrameStore {
  // TODO: QueueFrameStoreAdapter.getOrCreateQueue(path)

  private val fbIterator = if (total > 0) {
    new Iterator[FrameBatch] {
      private var remaining = total

      override def hasNext: Boolean = remaining > 0

      override def next(): FrameBatch = {
        if (hasNext) {
          remaining -= 1
          //          println("taking from queue:" + path)
          val ret = QueueFrameStore.getOrCreateQueue(path).take()
          //          println("token from queue:" + path)
          ret
        }
        else throw new NoSuchElementException("next on empty iterator")
      }
    }
  } else Iterator()

  override def close(): Unit = {}

  override def readAll(): Iterator[FrameBatch] = {
    require(total >= 0, "blocking queue need a total size before read")
    fbIterator
  }

  override def writeAll(batches: Iterator[FrameBatch]): Unit = {
    batches.foreach(QueueFrameStore.getOrCreateQueue(path).put)
  }
}

class NetworkFrameStore(path: String, host: String, port: Int) extends FrameStore {
  var client: NioTransferEndpoint = _
  val fbIterator: Iterator[FrameBatch] = {
    // read FrameBatch from local queue, if FrameBatch was used many time, must be loaded to cache
    new Iterator[FrameBatch] {
      // TODO: only send/receive one batch currently. If add a variable named 'total' like queueFrameDB, can't keep a union
      //       apply function of FrameDB.
      private var remaining = 1

      override def hasNext: Boolean = remaining > 0

      override def next(): FrameBatch = {
        if (hasNext) {
          remaining -= 1
          val ret = QueueFrameStore.getOrCreateQueue(path).take
          ret
        } else throw new NoSuchElementException("next on empty iterator")
      }
    }
  }

  override def writeAll(batches: Iterator[FrameBatch]): Unit = {
    // write FrameBatch to remote server
    var loop = 0
    var success: Boolean = false
    var error: Throwable = null
    if (client == null) {
      while (loop < FrameStore.NETWORK_CONNECT_NUM && !success) {
        try {
          client = new NioTransferEndpoint()
          client.runClient(host, port)
          success = true
        }
        catch {
          case e: Throwable => e.printStackTrace()
            loop += 1
            error = e
        }
      }
      if (loop >= FrameStore.NETWORK_CONNECT_NUM) {
        println(s"loop = $loop")

        throw new RuntimeException(s"Error to connect to EggRoll servers, Please try run again.", error)
      }
    }

    try {
      batches.foreach(batch => client.send(path, batch))
    } catch {
      case e: Throwable => e.printStackTrace()
        throw new IOException(s"Write FrameBatch to Network failed,server host:$host,port:$port", e)
    }
  }

  override def readAll(): Iterator[FrameBatch] = fbIterator

  override def close(): Unit = {
    // need to close ?
    client.clientChannel.close()
  }
}

object ArtificialDirectBuffer{
  private val cache: TrieMap[String,ByteBuffer] = new TrieMap[String,ByteBuffer]()
  def putAndUpdate(path:String,buffer:ByteBuffer): Unit ={
    cache.put(path,buffer)
  }

  def remove(path:String): Unit ={
    if (cache.contains(path)){
      PlatformDependent.freeDirectBuffer(cache(path))
      cache.remove(path)
    }
  }

  def get(path:String): ByteBuffer ={
    cache(path)
  }

  def contain(path:String): Boolean ={
    cache.contains(path)
  }
}

object JvmFrameStore {
  private val caches: TrieMap[String, ListBuffer[FrameBatch]] = new TrieMap[String, ListBuffer[FrameBatch]]()
  private val persistence: mutableSet[String] = mutableSet[String]()

  def remove(path: String): Unit = {
    val fb = caches(path).head
    fb.clear()
    caches.-=(path)
    println(s"clear:$path")
    assert(fb.isEmpty)
  }

  def printJvmFrameStore(): Unit = {
    println("-----------------------")
    caches.foreach(i => println(i._1))
    println("-----------------------")
  }

  def getJvmFrameBatches(dir: String): collection.Map[String, ListBuffer[FrameBatch]] = {
    JvmFrameStore.caches.filterKeys(_.toLowerCase.startsWith(dir.toLowerCase))
  }

  def checkFrameBatch(path: String): Boolean = {
    JvmFrameStore.caches.contains(path)
  }

  def addExclude(path: String): Unit = {
    persistence += path
  }

  def deleteExclude(path: String): Unit = {
    persistence -= path
  }

  def reSetExclude(): Unit = {
    persistence.clear()
  }

  def release(): Unit = {
    new Thread() {
      override def run(): Unit = {
        try {
          if (persistence.nonEmpty) {
            println("persistence:")
            persistence.foreach(i => println(i))
            for (element <- caches) {
              var isClean = true
              for (path <- persistence) {
                if (element._1.toLowerCase.startsWith(path.toLowerCase)) {
                  isClean = false
                }
              }
              if (isClean) {
                println(s"clear:${element._1}")
                element._2.foreach { i =>
                  i.clear()
                  assert(i.isEmpty)
                }
                caches.-=(element._1)
              }
            }
          } else {
            for (element <- caches) {
              println(s"clear:${element._1}")
              element._2.foreach { i =>
                i.clear()
                assert(i.isEmpty)
              }
              caches.-=(element._1)
            }
          }
          println("success to clear")
        } catch {
          case e: Throwable =>
            println("release store failed")
            e.printStackTrace()
        }
      }
    }.start()
  }
}

class JvmFrameStore(path: String) extends FrameStore {
  override def readAll(): Iterator[FrameBatch] = {
    try {
      JvmFrameStore.caches(path).toIterator
    } catch {
      case t: Throwable => t.printStackTrace()
        throw new NoSuchElementException(s"path:$path isn't in jvm frame store")
    }
  }

  override def writeAll(batches: Iterator[FrameBatch]): Unit = this.synchronized {
    if (!JvmFrameStore.caches.contains(path)) {
      JvmFrameStore.caches.put(path, ListBuffer())
    }
    JvmFrameStore.caches(path).appendAll(batches)
  }

  // TODO : clear ?
  override def close(): Unit = {}
}

class ArrowStreamSiblingWriter(root: VectorSchemaRoot,
                               provider: DictionaryProvider,
                               out: WritableByteChannel,
                               var nullableFields: Set[Int] = Set[Int]())
  extends ArrowStreamWriter(root, provider, out: WritableByteChannel) {

  def this(root: VectorSchemaRoot, provider: DictionaryProvider, outStream: OutputStream) {
    this(root, provider, Channels.newChannel(outStream))
  }

  def writeSibling(sibling: VectorSchemaRoot): Unit = {
    val vu = new VectorUnloader(root)
    val batch = vu.getRecordBatch
    writeRecordBatch(batch)
  }
}

class ArrowStreamReusableReader(messageReader: MessageChannelReader,
                                allocator: BufferAllocator,
                                var nullableFields: Set[Int] = Set[Int]())
  extends ArrowStreamReader(messageReader, allocator) {

  def this(in: InputStream, allocator: BufferAllocator) {
    this(new MessageChannelReader(new ReadChannel(Channels.newChannel(in)), allocator), allocator)
  }

  def this(in: ReadableByteChannel, allocator: BufferAllocator) {
    this(new MessageChannelReader(new ReadChannel(in), allocator), allocator)
  }

  override def loadNextBatch(): Boolean =
    throw new UnsupportedOperationException("use loadNewBatch instead")

  // do not reset row count
  override def prepareLoadNextBatch(): Unit = {
    ensureInitialized()
  }

  // For concurrent usage
  def loadNewBatch(): VectorSchemaRoot = {
    prepareLoadNextBatch()
    val result = messageReader.readNext

    // Reached EOS
    if (result == null) return null

    if (result.getMessage.headerType != MessageHeader.RecordBatch)
      throw new IOException("Expected RecordBatch but header was " + result.getMessage.headerType)

    var bodyBuffer = result.getBodyBuffer

    // For zero-length batches, need an empty buffer to deserialize the batch
    if (bodyBuffer == null) bodyBuffer = allocator.getEmpty

    val batch = MessageSerializer.deserializeRecordBatch(result.getMessage, bodyBuffer)

    val dr = new ArrowDiscreteReader(this.getVectorSchemaRoot.getSchema, batch, allocator)
    dr.loadNextBatch()
    dr.getVectorSchemaRoot
  }
}

class ArrowDiscreteReader(schema: Schema, batch: ArrowRecordBatch, allocator: BufferAllocator)
  extends ArrowReader(allocator) {
  private var hasNext = true

  // once only
  override def loadNextBatch(): Boolean = {
    ensureInitialized()
    loadRecordBatch(batch)
    hasNext = false
    hasNext
  }

  override def bytesRead(): Long =
    throw new UnsupportedOperationException("use loadNextBatch")

  override def closeReadSource(): Unit =
    throw new UnsupportedOperationException("use loadNextBatch")

  override def readSchema(): Schema = schema

  //  override def readDictionary(): ArrowDictionaryBatch =
  //    throw new UnsupportedOperationException("use loadNextBatch")
}

class FrameWriter(val rootSchema: VectorSchemaRoot, val arrowWriter: ArrowStreamSiblingWriter) {

  var outputStream: OutputStream = _

  def this(rootSchema: VectorSchemaRoot, outputStream: OutputStream) {
    this(rootSchema,
      new ArrowStreamSiblingWriter(
        rootSchema,
        new DictionaryProvider.MapDictionaryProvider(),
        outputStream
      )
    )
    this.outputStream = outputStream
  }

  def this(rootSchema: VectorSchemaRoot, channel: WritableByteChannel) {
    this(rootSchema,
      new ArrowStreamSiblingWriter(
        rootSchema,
        new DictionaryProvider.MapDictionaryProvider(),
        channel
      )
    )
  }

  def this(frameBatch: FrameBatch, adapter: BlockDeviceAdapter) {
    this(frameBatch.rootSchema.arrowSchema, adapter.getOutputStream())
  }

  def this(frameBatch: FrameBatch, outputStream: OutputStream) {
    this(frameBatch.rootSchema.arrowSchema, outputStream)
  }

  def this(frameBatch: FrameBatch, channel: WritableByteChannel) {
    this(frameBatch.rootSchema.arrowSchema, channel)
  }

  def this(rootSchema: FrameSchema, adapter: BlockDeviceAdapter) {
    this(rootSchema.arrowSchema, adapter.getOutputStream())
  }

  def close(closeStream: Boolean = true): Unit = {
    // Output Stream can be closed in arrowWriter ?
    arrowWriter.end()
    if (closeStream) {
      arrowWriter.close()
      if (outputStream != null) outputStream.close()
    }
  }

  def write(valueCount: Int, batchSize: Int, f: (Int, FrameVector) => Unit): Unit = {
    arrowWriter.start()
    for (b <- 0 until (valueCount + batchSize - 1) / batchSize) {
      val rowCount = if (valueCount < batchSize) valueCount else batchSize
      rootSchema.setRowCount(rowCount)
      for (fieldIndex <- 0 until rootSchema.getSchema.getFields.size()) {
        val vector = rootSchema.getFieldVectors.get(fieldIndex)
        try {
          vector.setInitialCapacity(rowCount)
          vector.allocateNew()
          f(fieldIndex, FrameVector(vector))
          vector.setValueCount(rowCount)
        } finally {
          // todo: should be closed with config?
          // vect.close()
        }
      }
      arrowWriter.writeBatch()
    }
  }

  // existed vectors
  def write(): Unit = {
    rootSchema.setRowCount(rootSchema.getFieldVectors.get(0).getValueCount)
    arrowWriter.writeBatch()
  }

  def writeSibling(columnarBatch: FrameBatch): Unit = {
    val sibling = new VectorSchemaRoot(
      columnarBatch.rootVectors.map(_.fieldVector).toIterable.asJava)
    sibling.setRowCount(sibling.getFieldVectors.get(0).getValueCount)
    arrowWriter.writeSibling(sibling)
  }
}
