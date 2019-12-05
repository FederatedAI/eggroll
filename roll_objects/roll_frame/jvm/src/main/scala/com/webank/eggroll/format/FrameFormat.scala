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
import java.nio.channels.ReadableByteChannel

import com.webank.eggroll.core.io.adapter.BlockDeviceAdapter
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector}
import org.apache.arrow.vector.types.pojo.Schema

import scala.collection.JavaConverters._

class FrameFormat {

}

class FrameBatch(val rootSchema: FrameSchema,
                 allocateNewRows: Int = -1,
                 virtualRowStart: Int = 0,
                 virtualRowCount: Int = -1) {
  val rootVectors: Array[FrameVector] = if (virtualRowCount > 0) {
    rootSchema.columnarVectors.map(
      v => new FrameVector(v.fieldVector, virtualRowStart, virtualRowCount))
  } else {
    rootSchema.columnarVectors
  }

  if (allocateNewRows > 0) {
    rootSchema.arrowSchema.setRowCount(allocateNewRows)
  }

  val fieldCount: Int = rootSchema.columnarVectors.length

  def rowCount: Int =
    if (virtualRowCount > 0) virtualRowCount else rootSchema.arrowSchema.getRowCount

  def sliceByColumn(from: Int, to: Int): FrameBatch =
    new FrameBatch(rootSchema.slice(from, to))

  def spareByColumn(fieldCount: Int, from: Int, to: Int): FrameBatch =
    new FrameBatch(rootSchema.sparse(fieldCount, from, to))

  def sliceByRow(from: Int, to: Int): FrameBatch = {
    require(to >= from, s"from: $from should > to: $to")
    new FrameBatch(rootSchema, virtualRowStart = from, virtualRowCount = to - from)
  }

  /**
    * Slice this root at desired index and length.
    * But FieldVector dataBufferAddress is the same.
    * @param index  start position of the slice
    * @param length length of the slice
    * @return the sliced root
    */
  def sliceRealByRow(index: Int, length: Int): FrameBatch = {
    require(index >= 0, s"index: $index should >= 0")
    require(length >= 0, s"length: $length should >= 0")
    new FrameBatch(new FrameSchema(rootSchema.arrowSchema.slice(index, length)))
  }

  def readDouble(field: Int, row: Int): Double = rootVectors(field).readDouble(row)

  def writeDouble(field: Int, row: Int, item: Double): Unit =
    rootVectors(field).writeDouble(row, item)

  def readLong(field: Int, row: Int): Long = rootVectors(field).readLong(row)

  def writeLong(field: Int, row: Int, item: Long): Unit = rootVectors(field).writeLong(row, item)

  def readInt(field: Int, row: Int): Int = rootVectors(field).readInt(row)

  def writeInt(field: Int, row: Int, item: Int): Unit = rootVectors(field).writeInt(row, item)

  def getArray(field: Int, row: Int): FrameVector = rootVectors(field).getArray(row)

  def getList(field: Int, row: Int, initialSize: Int = -1): FrameVector =
    rootVectors(field).getList(row, initialSize)
}

class FrameVector(val fieldVector: FieldVector,
                  virtualRowStart: Int = 0,
                  virtualRowCount: Int = -1) {
  private var nullable = false

  def isNullable: Boolean = nullable

  def setNullable(value: Boolean): Unit = nullable = value

  def valueCount: Int = if (virtualRowCount > 0) virtualRowCount else fieldVector.getValueCount

  def valueCount(count: Int): Unit = {
    fieldVector.setInitialCapacity(count)
    fieldVector.allocateNew()
    fieldVector.setValueCount(count)
  }

  // TODO: not safe set?
  def readDouble(index: Int): Double =
    fieldVector.asInstanceOf[Float8Vector].get(index + virtualRowStart)

  def writeDouble(index: Int, item: Double): Unit =
    fieldVector.asInstanceOf[Float8Vector].setSafe(index + virtualRowStart, item)

  def readLong(index: Int): Long =
    fieldVector.asInstanceOf[BigIntVector].get(index + virtualRowStart)

  def writeLong(index: Int, item: Long): Unit =
    fieldVector.asInstanceOf[BigIntVector].setSafe(index + virtualRowStart, item)

  def readInt(index: Int): Int = fieldVector.asInstanceOf[IntVector].get(index + virtualRowStart)

  def writeInt(index: Int, item: Int): Unit =
    fieldVector.asInstanceOf[IntVector].setSafe(index + virtualRowStart, item)

  def getArray(index: Int): FrameVector = getList(index)

  // initialSize == -1 means should not allocate new memory
  def getList(index: Int, initialSize: Int = -1): FrameVector = {
    val realIndex = index + virtualRowStart
    fieldVector match {
      case fv: FixedSizeListVector =>
        new FrameListVector(fv,
          realIndex * fv.getListSize,
          realIndex * fv.getListSize + fv.getListSize)

      case fv: ListVector =>
        if (initialSize > 0) {
          if (realIndex < fv.getLastSet) {
            throw new IllegalStateException(
              s"can not reinit list: lastSet:${fv.getLastSet}, index:(${index}, ${realIndex})")
          }
          fv.startNewValue(realIndex)
          fv.endValue(realIndex, initialSize)
        }
        new FrameListVector(fv, fv.getOffsetBuffer.getInt(realIndex * 4),
          fv.getOffsetBuffer.getInt((realIndex + 1) * 4))

      case _ => throw new UnsupportedOperationException("to do")
    }
  }

}

class FrameListVector(fieldVector: FieldVector, val startOffset: Int, val endOffset: Int)
  extends FrameVector(fieldVector.getChildrenFromFields.get(0)) {
  // override def valueCount(count: Int): Unit = throw new UnsupportedOperationException("can't change value count")
  // override def valueCount(count: Int): Unit = fieldVector.asInstanceOf[ListVector].startNewValue(count)

  override def valueCount: Int = endOffset - startOffset

  override def readDouble(index: Int): Double = super.readDouble(startOffset + index)

  override def writeDouble(index: Int, item: Double): Unit =
    super.writeDouble(startOffset + index, item)

  override def readLong(index: Int): Long = super.readLong(startOffset + index)

  override def writeLong(index: Int, item: Long): Unit = super.writeLong(startOffset + index, item)

  override def readInt(index: Int): Int = super.readInt(startOffset + index)

  override def writeInt(index: Int, item: Int): Unit = super.writeInt(startOffset + index, item)
}

class FrameReader(val arrowReader: ArrowStreamReusableReader,
                  var nullableFields: Set[Int] = Set[Int]()) {
  self =>

  def this(path: String) {
    this(new ArrowStreamReusableReader(
      new FileInputStream(path), new RootAllocator(Integer.MAX_VALUE)))
  }

  def this(adapter: BlockDeviceAdapter) {
    this(new ArrowStreamReusableReader(
      adapter.getInputStream(), new RootAllocator(Integer.MAX_VALUE)))
  }

  def this(inputStream: InputStream) {
    this(new ArrowStreamReusableReader(
      inputStream, new RootAllocator(Integer.MAX_VALUE)))
  }

  def this(in: ReadableByteChannel) {
    this(new ArrowStreamReusableReader(
      in, new RootAllocator(Integer.MAX_VALUE)))
  }

  def close(): Unit = arrowReader.close(true)

  def getColumnarBatches(): Iterator[FrameBatch] = {
    arrowReader.nullableFields = nullableFields
    new Iterator[FrameBatch] {
      var nextItem: VectorSchemaRoot = arrowReader.loadNewBatch()

      override def hasNext: Boolean = {
        nextItem != null
      }

      override def next(): FrameBatch = {
        if (nextItem == null) throw new NoSuchElementException("end")
        val ret = new FrameBatch(new FrameSchema(nextItem))
        nextItem = arrowReader.loadNewBatch()
        ret
      }
    }
  }
}

/**
  * root schema of a FrameBatch
  *
  * @param arrowSchema  : arrow root schema
  * @param fieldCount   : virtual root fields count
  * @param placeholders : index => actual index, value => virtual index
  */
class FrameSchema(val arrowSchema: VectorSchemaRoot,
                  fieldCount: Int = -1,
                  placeholders: Array[Int] = Array[Int]()) {
  def this(schema: String) {
    this(VectorSchemaRoot.create(Schema.fromJSON(schema), new RootAllocator(Integer.MAX_VALUE)))
  }

  def this(fieldVectors: Array[FrameVector],
           rowCount: Int,
           fieldCount: Int,
           placeholders: Array[Int]) {
    this(
      new VectorSchemaRoot(
        fieldVectors.map(_.fieldVector.getField).toList.asJava,
        fieldVectors.map(_.fieldVector).toList.asJava,
        rowCount),
      fieldCount,
      placeholders)
  }

  val columnarVectors: Array[FrameVector] = {
    if (fieldCount == -1 && placeholders.isEmpty) {
      arrowSchema.getFieldVectors.asScala.map(new FrameVector(_)).toArray
    } else {
      val ret = new Array[FrameVector](fieldCount)
      placeholders.zipWithIndex.foreach {
        case (n, i) => ret(n) = new FrameVector(arrowSchema.getFieldVectors.get(i))
      }
      ret
    }
  }

  def slice(from: Int, to: Int): FrameSchema = {
    require(to >= from, s"illegal arg. require to >= from. from: $from, to: $to")
    val fieldVectors = columnarVectors.zipWithIndex.filter {
      case (_, i) => i >= from && i < to
    }.map(_._1)
    val placeholders = (from until to).toArray
    new FrameSchema(fieldVectors, arrowSchema.getRowCount, columnarVectors.length, placeholders)
  }

  def sparse(fieldCount: Int, from: Int, to: Int): FrameSchema = {
    require(to >= from, s"illegal arg. require to >= from. from: $from, to: $to")
    val placeholders = (from until to).toArray
    new FrameSchema(columnarVectors, arrowSchema.getRowCount, fieldCount, placeholders)
  }
}
