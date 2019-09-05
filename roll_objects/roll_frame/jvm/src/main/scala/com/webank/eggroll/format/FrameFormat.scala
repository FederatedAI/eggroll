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
import java.nio.channels.Channels

import com.webank.eggroll.blockstore._
import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.FixedSizeListVector
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.message._
import org.apache.arrow.vector.ipc.{ArrowReader, ArrowStreamReader, ArrowStreamWriter, ReadChannel}
import org.apache.arrow.vector.types.pojo.Schema

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer

class FrameFormat {

}
object FrameUtil {
  def fromBytes(bytes:Array[Byte]):FrameBatch = {
    val input = new ByteArrayInputStream(bytes)
    val cr = new FrameReader(input)
    val it = cr.getColumnarBatches
    require(it.hasNext)
    val ret = it.next()
//    cr.close()
    ret
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
trait FrameStoreAdapter {
  def close():Unit
  def readAll():Iterator[FrameBatch]
  def writeAll(batches: Iterator[FrameBatch]):Unit
  def append(batch:FrameBatch): Unit = writeAll(Iterator(batch))
}
object FrameStoreAdapter {
  def apply(opts:Map[String, String]): FrameStoreAdapter = opts.getOrElse("type", "file") match {
    case "jvm" => new JvmFrameStoreAdapter(opts("path"))
    case _ => new FileFrameStoreAdapter(opts("path"))
  }
}
// NOT thread safe
class FileFrameStoreAdapter(path: String) extends FrameStoreAdapter {
  var frameReader:FrameReader = _
  var frameWriter:FrameWriter = _
  def close():Unit = {
    if(frameReader != null) frameReader.close()
    if(frameWriter != null) frameWriter.close()
  }

  def readAll():Iterator[FrameBatch] = {
    frameReader = new FrameReader(new FileStoreAdapter(path))
    frameReader.getColumnarBatches
  }

  def writeAll(batches: Iterator[FrameBatch]): Unit = {
    batches.foreach{ batch =>
      if(frameWriter == null) {
        frameWriter = new FrameWriter(batch, new FileStoreAdapter(path))
        frameWriter.write()
      } else {
        frameWriter.writeSibling(batch)
      }
    }
  }
}
object JvmFrameStoreAdapter {
  private val caches: TrieMap[String, ListBuffer[FrameBatch]] = new TrieMap[String, ListBuffer[FrameBatch]]()
}
class JvmFrameStoreAdapter(path: String) extends FrameStoreAdapter  {

  override def readAll(): Iterator[FrameBatch] = JvmFrameStoreAdapter.caches(path).toIterator

  override def writeAll(batches: Iterator[FrameBatch]): Unit = this.synchronized {
    if(!JvmFrameStoreAdapter.caches.contains(path)) {
      JvmFrameStoreAdapter.caches.put(path, ListBuffer())
    }
    JvmFrameStoreAdapter.caches(path).appendAll(batches)
  }
  // TODO : clear ?
  override def close(): Unit = {}
}
class FrameBatch(val columnarVectors: Array[FrameVector], var rowCount:Int = -1) {
  def this(columnarSchema: FrameSchema, rowCount:Int){
    this(columnarSchema.columnarVectors.toArray, rowCount)
    allocateNew()
  }
  private def allocateNew():Unit = {
    require(rowCount >= 0)
    columnarVectors.foreach(_.valueCount(rowCount))
  }

  def readDouble(field:Int, row: Int):Double = columnarVectors(field).readDouble(row)

  def writeDouble(field:Int, row: Int, item: Double): Unit = columnarVectors(field).writeDouble(row, item)

  def readLong(field:Int, row: Int):Long = columnarVectors(field).readLong(row)

  def writeLong(field:Int, row: Int, item: Long): Unit = columnarVectors(field).writeLong(row, item)

  def readInt(field:Int, row: Int):Int = columnarVectors(field).readInt(row)

  def writeInt(field:Int, row: Int, item: Int): Unit = columnarVectors(field).writeInt(row, item)

  // TODO: create when init schema
//  def createIntArray(field: Int): Unit = columnarVectors(field).asInstanceOf[FixedSizeListVector]
//    .addOrGetVector(new FieldType(false, new ArrowType.Int(32, true), null, null))
//
//  def createDoubleArray(field: Int): Unit = columnarVectors(field).asInstanceOf[FixedSizeListVector]
//    .addOrGetVector(new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null, null))
//
//  def createLongArray(field: Int): Unit = columnarVectors(field).asInstanceOf[FixedSizeListVector]
//    .addOrGetVector(FieldType.nullable(new ArrowType.Int(64, true)))
//
  def getValueVector(field: Int,  row:Int): FrameArrayVector = columnarVectors(field).getValueVector(row)
}

class FrameVector(val fieldVector: FieldVector) {
  def valueCount:Int = fieldVector.getValueCount
  def valueCount(count: Int): Unit = {
    fieldVector.setInitialCapacity(count)
    fieldVector.allocateNew()
    fieldVector.setValueCount(count)
  }
  // TODO:  not safe set
  def readDouble(index: Int):Double = fieldVector.asInstanceOf[Float8Vector].get(index)
  def writeDouble(index:Int, item: Double):Unit = fieldVector.asInstanceOf[Float8Vector].setSafe(index, item)
  def readLong(index:Int):Long = fieldVector.asInstanceOf[BigIntVector].get(index)
  def writeLong(index:Int, item: Long):Unit = fieldVector.asInstanceOf[BigIntVector].setSafe(index, item)
  def readInt(index:Int):Int = fieldVector.asInstanceOf[IntVector].get(index)
  def writeInt(index:Int, item: Int):Unit = fieldVector.asInstanceOf[IntVector].setSafe(index, item)

  def getValueVector(index: Int) = new FrameArrayVector(fieldVector.asInstanceOf[FixedSizeListVector], index)

}

class FrameArrayVector(fieldVector: FixedSizeListVector, val rowIndex:Int)
  extends FrameVector(fieldVector.getDataVector) {
  override def valueCount(count: Int): Unit = throw new UnsupportedOperationException("can't change value count")

  override def valueCount: Int = fieldVector.getListSize

  override def readDouble(index: Int): Double = super.readDouble(rowIndex * valueCount + index)

  override def writeDouble(index: Int, item: Double): Unit = super.writeDouble(rowIndex * valueCount + index, item)

  override def readLong(index: Int): Long = super.readLong(rowIndex * valueCount + index)
  override def writeLong(index: Int, item: Long): Unit = super.writeLong(rowIndex * valueCount + index, item)
  override def readInt(index: Int): Int = super.readInt(rowIndex * valueCount + index)
  override def writeInt(index: Int, item: Int): Unit = super.writeInt(rowIndex * valueCount + index, item)
}

class FrameReader(val arrowReader: ArrowStreamReusableReader) { self =>
  def this(path: String) {
    this(new ArrowStreamReusableReader(
      new FileInputStream(path), new RootAllocator(Integer.MAX_VALUE)))
  }
  def this(adapter: BlockStoreAdapter) {
    this(new ArrowStreamReusableReader(
      adapter.getInputStream(), new RootAllocator(Integer.MAX_VALUE)))
  }
  def this(inputStream: InputStream) {
    this(new ArrowStreamReusableReader(
      inputStream, new RootAllocator(Integer.MAX_VALUE)))
  }

  def close():Unit = {
    arrowReader.close(true)
  }

  def getColumnarBatches: Iterator[FrameBatch] = {
    new Iterator[FrameBatch] {
      var goon = false
      var nextItem:VectorSchemaRoot = _
      override def hasNext: Boolean = {
        nextItem = arrowReader.loadNewBatch()
        //        if(!goon) {
        //          close()
        //        }
        nextItem != null
      }
      override def next(): FrameBatch = {
//        if(!goon) throw new Exception("end")
        // TODO: refactor
        if(nextItem == null) throw new Exception("end")
        new FrameBatch(
          nextItem.getFieldVectors.asScala.map(new FrameVector(_)).toArray,
          nextItem.getRowCount)
      }
    }
  }
}
class FrameSchema(val rootSchema: VectorSchemaRoot) {
  def this(schema: String) {
    this(VectorSchemaRoot.create(Schema.fromJSON(schema), new RootAllocator(Integer.MAX_VALUE)))
  }
  val columnarVectors: List[FrameVector] = rootSchema.getFieldVectors.asScala.map(new FrameVector(_)).toList

  //  def this(fields: List[ColumnarVector], path:String) {
  //    this(new VectorSchemaRoot(fields.map(_.fieldVector).asJava))
  //  }
  //
  //  def this(columnarBatch: ColumnarBatch, adapter: BlockDeviceAdapter) {
  //    this(new VectorSchemaRoot(columnarBatch.columnarVectors.map(_.fieldVector).asJava))
  //  }
}
class ArrowStreamSiblingWriter(root: VectorSchemaRoot, provider: DictionaryProvider, outStream: OutputStream)
  extends ArrowStreamWriter(root, provider, outStream) {

  def writeSibling(sibling: VectorSchemaRoot): Unit = {
    val vu = new VectorUnloader(root)
    val batch = vu.getRecordBatch
    writeRecordBatch(batch)
  }

  override def end(): Unit = {
    this.endInternal(out)
  }

}
class ArrowStreamReusableReader(messageReader: MessageChannelReader, allocator: BufferAllocator)
  extends ArrowStreamReader(messageReader, allocator) {
//  private var messageReaderCopy: MessageChannelReader = _
//  def initMessageReader(in:InputStream): MessageChannelReader = {
//    messageReaderCopy =  new MessageChannelReader(new ReadChannel(Channels.newChannel(in)), allocator)
//    messageReaderCopy
//  }
  def this(in: InputStream, allocator: BufferAllocator) {
    this(new MessageChannelReader(new ReadChannel(Channels.newChannel(in)), allocator), allocator)
  }
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

class ArrowDiscreteReader(schema: Schema, batch: ArrowRecordBatch, allocator: BufferAllocator) extends ArrowReader(allocator){
  private var hasNext = true
  // once only
  override def loadNextBatch(): Boolean = {
    ensureInitialized()
    loadRecordBatch(batch)
    hasNext = false
    hasNext
  }

  override def bytesRead(): Long = ???

  override def closeReadSource(): Unit = ???

  override def readSchema(): Schema = schema

  override def readDictionary(): ArrowDictionaryBatch = ???
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
  def this(rootSchema: VectorSchemaRoot, path:String) {
    this(rootSchema, new FileOutputStream(path))
    new File(path).getParentFile.mkdirs()
  }
  def this(schema: String, path: String) {
    this(VectorSchemaRoot.create(Schema.fromJSON(schema), new RootAllocator(Integer.MAX_VALUE)), path)
  }

  def this(fields: List[FrameVector], path:String) {
    this(new VectorSchemaRoot(fields.map(_.fieldVector).asJava),path)
  }

  def this(columnarBatch: FrameBatch, adapter: BlockStoreAdapter) {
    this(new VectorSchemaRoot(columnarBatch.columnarVectors.map(_.fieldVector).toIterable.asJava), adapter.getOutputStream())
  }
  def this(columnarBatch: FrameBatch, outputStream: OutputStream) {
    this(new VectorSchemaRoot(columnarBatch.columnarVectors.map(_.fieldVector).toIterable.asJava), outputStream)
  }
  def this(schema: String, adapter: BlockStoreAdapter) {
    this(VectorSchemaRoot.create(Schema.fromJSON(schema), new RootAllocator(Integer.MAX_VALUE)), adapter.getOutputStream())
  }

  def close():Unit = {
    // Output Stream can be closed in arrowWriter ?
    arrowWriter.end()
    arrowWriter.close()
    outputStream.close()
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
          // should not be closed
          //          vect.close()
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
    val sibling = new VectorSchemaRoot(columnarBatch.columnarVectors.map(_.fieldVector).toIterable.asJava)
    sibling.setRowCount(sibling.getFieldVectors.get(0).getValueCount)
    arrowWriter.writeSibling(sibling)
  }

}
