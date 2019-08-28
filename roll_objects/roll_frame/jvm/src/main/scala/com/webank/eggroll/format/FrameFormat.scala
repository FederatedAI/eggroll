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

class FrameFormat {

}


import java.io._
import java.nio.channels.{Channels, ReadableByteChannel}

import com.webank.eggroll.blockstore._
import io.netty.buffer.ArrowBuf
import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{FieldVector, Float8Vector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.message._
import org.apache.arrow.vector.ipc.{ArrowReader, ArrowStreamReader, ArrowStreamWriter, ReadChannel}
import org.apache.arrow.vector.types.pojo.Schema

import scala.collection.JavaConverters._

class EggrollFormat {

}

object ColumnarUtil {
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

}

class FrameBatch(val columnarVectors:List[FrameVector], var rowCount:Int = -1) {
  def this(columnarSchema: FrameSchema, rowCount:Int){
    this(columnarSchema.columnarVectors, rowCount)
    allocateNew()
  }
  def readDouble(field:Int, row: Int):Double = {
    columnarVectors(field).readDouble(row)
  }
  private def allocateNew():Unit = {
    require(rowCount >= 0)
    columnarVectors.foreach(_.valueCount(rowCount))
  }
  def writeDouble(field:Int, row: Int, item: Double): Unit = {
    columnarVectors(field).writeDouble(row, item)
  }
}

class FrameVector(val fieldVector: FieldVector) {
  def valueCount:Int = fieldVector.getValueCount
  def valueCount(count: Int): Unit = {
    fieldVector.setInitialCapacity(count)
    fieldVector.allocateNew()
    fieldVector.setValueCount(count)
  }
  def readDouble(index: Int):Double = fieldVector.asInstanceOf[Float8Vector].get(index)
  def writeDouble(index:Int, item: Double):Unit = fieldVector.asInstanceOf[Float8Vector].setSafe(index, item)
}

class FrameReader(val arrowReader: ArrowReader) { self =>
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
      override def hasNext: Boolean = {
        goon = arrowReader.loadNextBatch()
        //        if(!goon) {
        //          close()
        //        }
        goon
      }
      override def next(): FrameBatch = {
        if(!goon) throw new Exception("end")
        new FrameBatch(
          arrowReader.getVectorSchemaRoot.getFieldVectors.asScala.map(new FrameVector(_)).toList,
          arrowReader.getVectorSchemaRoot.getRowCount)
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
class ArrowStreamReusableReader(in: InputStream, allocator: BufferAllocator)
  extends ArrowStreamReader(in, allocator) {
  // do not reset row count
  override def prepareLoadNextBatch(): Unit = {
    ensureInitialized()
  }
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
    this(new VectorSchemaRoot(columnarBatch.columnarVectors.map(_.fieldVector).asJava), adapter.getOutputStream())
  }
  def this(columnarBatch: FrameBatch, outputStream: OutputStream) {
    this(new VectorSchemaRoot(columnarBatch.columnarVectors.map(_.fieldVector).asJava), outputStream)
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
    val sibling = new VectorSchemaRoot(columnarBatch.columnarVectors.map(_.fieldVector).asJava)
    sibling.setRowCount(sibling.getFieldVectors.get(0).getValueCount)
    arrowWriter.writeSibling(sibling)
  }

}
