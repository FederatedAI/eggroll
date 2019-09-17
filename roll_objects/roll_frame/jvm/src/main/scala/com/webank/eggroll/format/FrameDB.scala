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

package com.webank.eggroll.format

import java.io._
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.webank.eggroll.blockdevice.{BlockDeviceAdapter, FileBlockAdapter}
import com.webank.eggroll.rollframe.RfStore
import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.message._
import org.apache.arrow.vector.ipc.{ArrowReader, ArrowStreamReader, ArrowStreamWriter, ReadChannel}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer


object FrameUtil {
  def fromBytes(bytes:Array[Byte]):FrameBatch = {
    val input = new ByteArrayInputStream(bytes)
    val cr = new FrameReader(input)
    cr.getColumnarBatches().next()
  }

  def toBytes(cb: FrameBatch):Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val cw = new FrameWriter(cb, output)
    cw.write()
    val ret = output.toByteArray
    cw.close()
    ret
  }

  def copy(fb: FrameBatch): FrameBatch = {
    fromBytes(toBytes(fb))
  }
}
// TODO: delete ?
trait FrameDB {
  def close():Unit
  def readAll():Iterator[FrameBatch]
  def writeAll(batches: Iterator[FrameBatch]):Unit
  def append(batch:FrameBatch): Unit = writeAll(Iterator(batch))
  def readOne():FrameBatch = readAll().next()
}

object FrameDB {
  val TYPE_FILE = "file"
  val TYPE_CACHE = "cache"
  val TYPE_QUEUE = "queue"
  private val rootPath = "./tmp/unittests/RollFrameTests/filedb/"

  private def getStorePath(rfStore: RfStore, partId: Int): String = {
    val dir = if (rfStore.path == null || rfStore.path.isEmpty)
      rootPath + "/" + rfStore.namespace + "/" + rfStore.name
    else rfStore.path
    dir + "/" + partId
  }

  def apply(opts: Map[String, String]): FrameDB = opts.getOrElse("type", "file") match {
    case TYPE_CACHE => new JvmFrameDB(opts("path"))
    case TYPE_QUEUE => new QueueFrameDB(opts("path"), opts("total").toInt)
    case _ => new FileFrameDB(opts("path"))
  }

  def apply(rfStore: RfStore, part: Int): FrameDB =
    apply(Map("path" -> getStorePath(rfStore, part), "type" -> rfStore.storeType))

  def queue(path: String, total: Int): FrameDB = apply(Map("path" -> path, "total" -> total.toString, "type" -> TYPE_QUEUE))

  def file(path: String): FrameDB = apply(Map("path" -> path, "type" -> TYPE_FILE))

  def cache(path: String): FrameDB = apply(Map("path" -> path, "type" -> TYPE_CACHE))
}

// NOT thread safe
class FileFrameDB(path: String) extends FrameDB {
  var frameReader:FrameReader = _
  var frameWriter:FrameWriter = _
  def close():Unit = {
    if(frameReader != null) frameReader.close()
    if(frameWriter != null) frameWriter.close()
  }

  def readAll():Iterator[FrameBatch] = {
    frameReader = new FrameReader(new FileBlockAdapter(path))
    frameReader.getColumnarBatches()
  }
  def readAll(nullableFields:Set[Int] = Set[Int]()):Iterator[FrameBatch] = {
    val dir = new File(path).getParentFile
    if(!dir.exists()) dir.mkdirs()
    frameReader = new FrameReader(new FileBlockAdapter(path))
    frameReader.nullableFields = nullableFields
    frameReader.getColumnarBatches()
  }

  def writeAll(batches: Iterator[FrameBatch]): Unit = {
    batches.foreach{ batch =>
      if(frameWriter == null) {
        frameWriter = new FrameWriter(batch, new FileBlockAdapter(path))
        frameWriter.write()
      } else {
        frameWriter.writeSibling(batch)
      }
    }
  }
}
object QueueFrameDB {
  private val map = TrieMap[String, BlockingQueue[FrameBatch]]()

  def getOrCreateQueue(key: String):BlockingQueue[FrameBatch] = this.synchronized {
    if(!map.contains(key)) {
      map.put(key, new LinkedBlockingQueue[FrameBatch]())
    }
    map(key)
  }
}
class QueueFrameDB(path:String, total: Int) extends FrameDB {
  // TODO: QueueFrameStoreAdapter.getOrCreateQueue(path)
  override def close(): Unit = {}

  override def readAll(): Iterator[FrameBatch] = {
    require(total >= 0, "blocking queue need a total size before read")
    new Iterator[FrameBatch] {
      private var remaining = total
      override def hasNext: Boolean = remaining > 0

      override def next(): FrameBatch = {
        remaining -= 1
        println("taking from queue:" + path)
        val ret = QueueFrameDB.getOrCreateQueue(path).take()
        println("token from queue:" + path)
        ret
      }
    }
  }

  override def writeAll(batches: Iterator[FrameBatch]): Unit = {
    batches.foreach(QueueFrameDB.getOrCreateQueue(path).put)
  }
}
object JvmFrameDB {
  private val caches: TrieMap[String, ListBuffer[FrameBatch]] = new TrieMap[String, ListBuffer[FrameBatch]]()
}

class JvmFrameDB(path: String) extends FrameDB  {

  override def readAll(): Iterator[FrameBatch] = JvmFrameDB.caches(path).toIterator

  override def writeAll(batches: Iterator[FrameBatch]): Unit = this.synchronized {
    if(!JvmFrameDB.caches.contains(path)) {
      JvmFrameDB.caches.put(path, ListBuffer())
    }
    JvmFrameDB.caches(path).appendAll(batches)
  }
  // TODO : clear ?
  override def close(): Unit = {}
}

class ArrowStreamSiblingWriter(root: VectorSchemaRoot, provider: DictionaryProvider, out: WritableByteChannel,
                               var nullableFields:Set[Int] = Set[Int]())
  extends ArrowStreamWriter(root, provider, out: WritableByteChannel) {
  def this(root: VectorSchemaRoot, provider: DictionaryProvider, outStream: OutputStream) ={
    this(root, provider, Channels.newChannel(outStream))
  }
  def writeSibling(sibling: VectorSchemaRoot): Unit = {
    val vu = new VectorUnloader(root)
    val batch = vu.getRecordBatch
    writeRecordBatch(batch)
  }
}
class ArrowStreamReusableReader(messageReader: MessageChannelReader, allocator: BufferAllocator,
                                var nullableFields:Set[Int] = Set[Int]())
  extends ArrowStreamReader(messageReader, allocator) {

  def this(in: InputStream, allocator: BufferAllocator) {
    this(new MessageChannelReader(new ReadChannel(Channels.newChannel(in)), allocator), allocator)
  }
  def this(in: ReadableByteChannel, allocator: BufferAllocator) {
    this(new MessageChannelReader(new ReadChannel(in), allocator), allocator)
  }
  override def loadNextBatch(): Boolean = throw new UnsupportedOperationException("use loadNewBatch instead")
  // do not reset row count
  override def prepareLoadNextBatch(): Unit = {
    ensureInitialized()
  }
  // For concurrent usage
  def loadNewBatch():VectorSchemaRoot = {
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
  extends ArrowReader(allocator){
  private var hasNext = true
  // once only
  override def loadNextBatch(): Boolean = {
    ensureInitialized()
    loadRecordBatch(batch)
    hasNext = false
    hasNext
  }

  override def bytesRead(): Long = throw new UnsupportedOperationException("use loadNextBatch")

  override def closeReadSource(): Unit = throw new UnsupportedOperationException("use loadNextBatch")

  override def readSchema(): Schema = schema

  override def readDictionary(): ArrowDictionaryBatch = throw new UnsupportedOperationException("use loadNextBatch")
}

class FrameWriter(val rootSchema: VectorSchemaRoot, val arrowWriter: ArrowStreamSiblingWriter) {

  var outputStream: OutputStream = _
  def this(rootSchema: VectorSchemaRoot, outputStream: OutputStream) {
    this(rootSchema,
      new ArrowStreamSiblingWriter(rootSchema, new DictionaryProvider.MapDictionaryProvider(),
        outputStream
      )
    )
    this.outputStream = outputStream
  }
  def this(rootSchema: VectorSchemaRoot, channel: WritableByteChannel) {
    this(rootSchema,
      new ArrowStreamSiblingWriter(rootSchema, new DictionaryProvider.MapDictionaryProvider(),
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

  def close(closeStream:Boolean = true):Unit = {
    // Output Stream can be closed in arrowWriter ?
    arrowWriter.end()
    if(closeStream){
      arrowWriter.close()
      if(outputStream != null) outputStream.close()
    }
  }

  def write(valueCount: Int, batchSize:Int, f: (Int, FrameVector) => Unit):Unit = {
    arrowWriter.start()
    for(b <- 0 until (valueCount + batchSize - 1) / batchSize) {
      val rowCount = if(valueCount < batchSize) valueCount else  batchSize
      rootSchema.setRowCount(rowCount)
      for(fieldIndex <- 0 until rootSchema.getSchema.getFields.size()){
        val vect = rootSchema.getFieldVectors.get(fieldIndex)
        try{
          vect.setInitialCapacity(rowCount)
          vect.allocateNew()
          f(fieldIndex, new FrameVector(vect))
          vect.setValueCount(rowCount)
        } finally {
          // should be closed with config?
          // vect.close()
        }
      }
      arrowWriter.writeBatch()
      println("batch index",b)
    }
  }
  // existed vectors
  def write():Unit = {
    rootSchema.setRowCount(rootSchema.getFieldVectors.get(0).getValueCount)
    arrowWriter.writeBatch()
  }
  def writeSibling(columnarBatch: FrameBatch):Unit = {
    val sibling = new VectorSchemaRoot(columnarBatch.rootVectors.map(_.fieldVector).toIterable.asJava)
    sibling.setRowCount(sibling.getFieldVectors.get(0).getValueCount)
    arrowWriter.writeSibling(sibling)
  }

}
