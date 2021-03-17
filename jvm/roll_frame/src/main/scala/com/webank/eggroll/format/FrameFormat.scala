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
 */

package com.webank.eggroll.format

import java.io._
import java.nio.ByteOrder
import java.nio.channels.ReadableByteChannel
import java.util.concurrent.{Callable, ThreadPoolExecutor}

import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.ThreadPoolUtils
import com.webank.eggroll.util.SchemaUtil
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector}
import org.apache.arrow.vector.types.pojo.Schema

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class FrameFormat {

}

/**
 * Store all data in a column
 */
@deprecated
class ColumnFrame(val fb: FrameBatch, val matrixCols: Int) {
  val matrixRows: Int = fb.rowCount / matrixCols

  def write(field: Int, row: Int, item: Double): Unit = {
    val index = field + row * matrixCols
    // use index to get which part and correct row id
    fb.writeDouble(0, index, item)
  }

  def read(field: Int, row: Int): Double = {
    val index = field + row * matrixCols
    fb.readDouble(0, index)
  }
}

/**
 * store block data in each partition
 *
 * @param rootSchema      root schema
 * @param allocateNewRows set all rows count
 * @param virtualRowStart begin index of virtual Row
 * @param virtualRowCount virtual rows count
 */

class FrameBatch(val rootSchema: FrameSchema,
                 allocateNewRows: Int = -1,
                 virtualRowStart: Int = 0,
                 virtualRowCount: Int = -1) {
  val rootVectors: Array[FrameVector] = if (virtualRowCount > 0) {
    rootSchema.columnarVectors.map(v =>
      FrameVector(v.fieldVector, virtualRowStart, virtualRowCount))
  } else {
    rootSchema.columnarVectors
  }

  if (allocateNewRows > 0) {
    rootSchema.arrowSchema.getFieldVectors.forEach { fv =>
      fv.setInitialCapacity(allocateNewRows)
      fv.allocateNew()
    }
    rootSchema.arrowSchema.setRowCount(allocateNewRows)
  }

  val fieldCount: Int = rootSchema.columnarVectors.length

  lazy val memorySize: Int = {
    var size = 0
    rootVectors.foreach { i =>
      size += i.fieldVector.getBufferSize
    }
    size / (1024 * 1024)
  }

  def rowCount: Int =
    if (virtualRowCount > 0) virtualRowCount else rootSchema.arrowSchema.getRowCount

  def isEmpty: Boolean ={
    rowCount == 0
  }

  /**
   * Mark the particular position in the vector as non-null. just use first initialization
   */
  def initZero(): Unit = {
    rootVectors.foreach { i =>
      i.fieldVector match {
        case fv: FixedSizeListVector =>
          val validityByteBuffer = i.getArray(0).fieldVector.getValidityBuffer
          validityByteBuffer.setBytes(0, Array.fill[Byte](validityByteBuffer.capacity().toInt)(-1))
        case fv: ListVector =>
          //   val validityByteBuffer = i.getArray(0).fieldVector.getValidityBuffer
          //   validityByteBuffer.setBytes(0, Array.fill[Byte](validityByteBuffer.capacity().toInt)(-1))
          throw new UnsupportedOperationException("Don't support ListVector init, except finishing init of values size")
        case _ =>
          val validityByteBuffer = i.fieldVector.getValidityBuffer
          validityByteBuffer.setBytes(0, Array.fill[Byte](validityByteBuffer.capacity().toInt)(-1))
      }
    }
  }

  /**
   * release direct buffer with block way
   */
  def clear(): Unit = {
    rootSchema.arrowSchema.clear()
  }

  /**
   * use a thread to release direct buffer, attention not to create the same name Frame Batch immediately.
   */
  val executorPool: ThreadPoolExecutor = ThreadPoolUtils.defaultThreadPool

  def close(): Unit = {
    new Thread() {
      override def run(): Unit = {
        try {
          clear()
        } catch {
          case e: Throwable =>
            println("close frame store failed")
            e.printStackTrace()
        }
      }
    }.start()
  }

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
   *
   * @param index  start position of the slice
   * @param length length of the slice
   * @return the sliced root
   */
  def sliceRealByRow(index: Int, length: Int): FrameBatch = {
    require(index >= 0, s"index: $index should >= 0")
    require(length >= 0, s"length: $length should >= 0")
    new FrameBatch(new FrameSchema(rootSchema.arrowSchema.slice(index, if (index + length > rowCount) rowCount - index else length)))
  }

  def readDouble(field: Int, row: Int): Double = rootVectors(field).readDouble(row)

  def writeDouble(field: Int, row: Int, item: Double): Unit = rootVectors(field).writeDouble(row, item)

  def readLong(field: Int, row: Int): Long = rootVectors(field).readLong(row)

  def writeLong(field: Int, row: Int, item: Long): Unit = rootVectors(field).writeLong(row, item)

  def readInt(field: Int, row: Int): Int = rootVectors(field).readInt(row)

  def writeInt(field: Int, row: Int, item: Int): Unit = rootVectors(field).writeInt(row, item)

  def getArray(field: Int, row: Int): FrameVector = rootVectors(field).getArray(row)

  def getList(field: Int, row: Int, initialSize: Int = -1): FrameVector =
    rootVectors(field).getList(row, initialSize)

  @deprecated
  private def getRowsList: List[Int] = {
    val realColumns = ColumnVectors.MAX_EACH_PART_SIZE / fieldCount
    if (realColumns > rowCount) {
      List(rowCount)
    } else {
      val parts = Math.ceil(rowCount.toDouble / realColumns).toInt
      (0 until parts).map { i =>
        if (i == parts - 1 && rowCount % realColumns != 0) {
          rowCount % realColumns
        }
        else {
          realColumns
        }
      }.toList
    }
  }


  // design choice
  // TODO: convert slowly
  def toColumnVectors: ColumnVectors = {
    // 1.求出每一个part数据的大小
    val columnVectors = new ColumnVectors(fieldCount)

    getRowsList.indices.foreach { i =>
      val columnVector = new ColumnVector(getRowsList(i), fieldCount)
      // 2.赋值
      for {f <- 0 until fieldCount
           r <- 0 until getRowsList(i)} {
        columnVector.writeDouble(r * fieldCount + f, this.readDouble(f, r))
      }
      columnVectors.addElement(columnVector)
    }
    columnVectors
  }
}

@deprecated
class ColumnVector(matrixRows: Int, matrixColumns: Int) {
  val allocator = new RootAllocator(Int.MaxValue)
  val fieldVector: Float8Vector = new Float8Vector("cv", allocator)
  private val count: Int = matrixRows * matrixColumns
  fieldVector.setInitialCapacity(count)
  fieldVector.allocateNew()
  fieldVector.setValueCount(count)

  def valueCount: Int = fieldVector.getValueCount

  def writeDouble(index: Int, item: Double): Unit = {
    fieldVector.set(index, item)
  }

  def readDouble(index: Int): Double = {
    fieldVector.get(index)
  }

  def getDataBufferAddress: Long = fieldVector.getDataBufferAddress
}

/**
 * package several ColumnVector with the same partition
 * Double type's max capacity is 134217728
 */
@deprecated
class ColumnVectors(val matrixColumns: Int) {
  val frames: ArrayBuffer[ColumnVector] = new ArrayBuffer[ColumnVector]

  def addElement(cv: ColumnVector): Unit = {
    frames += cv
  }

  /**
   * the number of ColumnVectors
   *
   * @return
   */
  def parts: Int = frames.size

  def getVector(i: Int): ColumnVector = frames(i)

  /**
   * all ColumnVectors' value size
   *
   * @return
   */
  def rowCount: Int = frames.foldLeft(0)((ac, nextColumnVector) => ac + nextColumnVector.valueCount)

  def matrixRows: Int = rowCount / matrixColumns
}

@deprecated
object ColumnVectors {
  val MAX_EACH_PART_SIZE = 134217727 //134217728
}

trait FrameVector {
  val fieldVector: FieldVector
  val virtualRowStart: Int
  val virtualRowCount: Int
  private var nullable = false

  def isNullable: Boolean = nullable

  def setNullable(value: Boolean): Unit = nullable = value

  def valueCount: Int = if (virtualRowCount > 0) virtualRowCount else fieldVector.getValueCount

  def valueCount(count: Int): Unit = {
    fieldVector.setInitialCapacity(count)
    fieldVector.allocateNew()
    fieldVector.setValueCount(count)
  }

  def getDataBufferAddress: Long = fieldVector.getDataBufferAddress

  def readDouble(index: Int): Double =
    fieldVector.asInstanceOf[Float8Vector].get(index + virtualRowStart)

  def writeDouble(index: Int, item: Double): Unit

  def readLong(index: Int): Long =
    fieldVector.asInstanceOf[BigIntVector].get(index + virtualRowStart)

  def writeLong(index: Int, item: Long): Unit

  def readInt(index: Int): Int = fieldVector.asInstanceOf[IntVector].get(index + virtualRowStart)

  def writeInt(index: Int, item: Int): Unit

  def getArray(index: Int): FrameVector = getList(index)

  // initialSize == -1 means should not allocate new memory
  def getList(index: Int, initialSize: Int = -1): FrameVector = {
    val realIndex = index + virtualRowStart
    fieldVector match {
      case fv: FixedSizeListVector =>
        FrameListVector(fv,
          realIndex * fv.getListSize,
          realIndex * fv.getListSize + fv.getListSize)

      case fv: ListVector =>
        if (initialSize > 0) {
          if (realIndex < fv.getLastSet) {
            throw new IllegalStateException(
              s"""can not reinit list: lastSet:${fv.getLastSet}, index:($index, $realIndex)""")
          }
          fv.startNewValue(realIndex)
          fv.endValue(realIndex, initialSize)
          fv.setValueCount(initialSize)
        }
        FrameListVector(fv, fv.getElementStartIndex(realIndex),
          fv.getElementEndIndex(realIndex))

      case _ => throw new UnsupportedOperationException("to do")
    }
  }
}

object FrameVector {
  private val UN_SAFE: Boolean = System.getProperty("arrow.enable_unsafe_memory_access", "false").toBoolean

  def apply(fieldVector: FieldVector, virtualRowStart: Int = 0, virtualRowCount: Int = -1): FrameVector = {
    if (UN_SAFE) new FrameVectorUnSafe(fieldVector, virtualRowStart, virtualRowCount) else
      new FrameVectorSafe(fieldVector, virtualRowStart, virtualRowCount)
  }
}

class FrameVectorUnSafe(val fieldVector: FieldVector,
                        val virtualRowStart: Int = 0,
                        val virtualRowCount: Int = -1) extends FrameVector {
  override def writeDouble(index: Int, item: Double): Unit =
    fieldVector.asInstanceOf[Float8Vector].set(index + virtualRowStart, item)

  override def writeLong(index: Int, item: Long): Unit =
    fieldVector.asInstanceOf[BigIntVector].set(index + virtualRowStart, item)

  override def writeInt(index: Int, item: Int): Unit =
    fieldVector.asInstanceOf[IntVector].set(index + virtualRowStart, item)
}

class FrameVectorSafe(val fieldVector: FieldVector,
                      val virtualRowStart: Int = 0,
                      val virtualRowCount: Int = -1) extends FrameVector {
  override def writeDouble(index: Int, item: Double): Unit =
    fieldVector.asInstanceOf[Float8Vector].setSafe(index + virtualRowStart, item)

  override def writeLong(index: Int, item: Long): Unit =
    fieldVector.asInstanceOf[BigIntVector].setSafe(index + virtualRowStart, item)

  override def writeInt(index: Int, item: Int): Unit =
    fieldVector.asInstanceOf[IntVector].setSafe(index + virtualRowStart, item)
}

/**
 * FrameListVector’s fieldVector is the same fieldVector and set row to  get part of fieldVector by offset
 */
object FrameListVector {
  private val UN_SAFE: Boolean = System.getProperty("arrow.enable_unsafe_memory_access", "false").toBoolean

  def apply(fieldVector: FieldVector, startOffset: Int, endOffset: Int): FrameVector = {
    if (UN_SAFE) new FrameListVectorUnSafe(fieldVector, startOffset, endOffset) else
      new FrameListVectorSafe(fieldVector, startOffset, endOffset)
  }
}


class FrameListVectorSafe(fieldVector: FieldVector, val startOffset: Int, val endOffset: Int)
  extends FrameVectorSafe(fieldVector.getChildrenFromFields.get(0)) {
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


class FrameListVectorUnSafe(fieldVector: FieldVector, val startOffset: Int, val endOffset: Int)
  extends FrameVectorUnSafe(fieldVector.getChildrenFromFields.get(0)) {
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
      new FileInputStream(path), new RootAllocator(Long.MaxValue)))
  }

  def this(adapter: BlockDeviceAdapter) {
    this(new ArrowStreamReusableReader(
      adapter.getInputStream(), new RootAllocator(Long.MaxValue)))
  }

  def this(inputStream: InputStream) {
    this(new ArrowStreamReusableReader(
      inputStream, new RootAllocator(Long.MaxValue)))
  }

  def this(in: ReadableByteChannel) {
    this(new ArrowStreamReusableReader(
      in, new RootAllocator(Long.MaxValue)))
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
    this(VectorSchemaRoot.create(Schema.fromJSON(schema), FrameSchema.rootAllocator))
  }

  def this(schema: Schema) {
    this(VectorSchemaRoot.create(schema, FrameSchema.rootAllocator))
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
      arrowSchema.getFieldVectors.asScala.map(FrameVector(_)).toArray
    } else {
      val ret = new Array[FrameVector](fieldCount)
      placeholders.zipWithIndex.foreach {
        case (n, i) => ret(n) = FrameVector(arrowSchema.getFieldVectors.get(i))
      }
      ret
    }
  }

  /**
   * placeholders value are FieldVector, and other place are null.
   *
   * @param from start position of all FieldVectors
   * @param to   end position of all FieldVectors
   * @return FrameSchema
   */
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

object FrameSchema {
  val rootAllocator = new RootAllocator(Long.MaxValue)
  val oneFieldSchema: Schema = Schema.fromJSON(SchemaUtil.oneDoubleFieldSchema)
}

object FrameUtils {
  /**
   * convert bytes data to FrameBatch
   *
   * @param bytes bytes data
   * @return
   */
  def fromBytes(bytes: Array[Byte]): FrameBatch = {
    val input = new ByteArrayInputStream(bytes)
    val cr = new FrameReader(input)
    cr.getColumnarBatches().next()
  }

  /**
   * convert FrameBatch to bytes data
   *
   * @param cb FrameBatch
   * @return
   */
  def toBytes(cb: FrameBatch): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val cw = new FrameWriter(cb, output)
    cw.write()
    val ret = output.toByteArray
    cw.close()
    ret
  }

  @deprecated("Use `FrameUtils.fork() instead`")
  def copy(fb: FrameBatch): FrameBatch = {
    fromBytes(toBytes(fb))
  }

  /**
   * transfer buffer to a new FrameBatch
   *
   * @param fb FrameBatch
   * @return
   */
  def transfer(fb: FrameBatch): FrameBatch = {
    val transfer: VectorSchemaRoot = {
      val sliceVectors = fb.rootSchema.arrowSchema.getFieldVectors.asScala.map((v: FieldVector) => {
        def foo(v: FieldVector): FieldVector = {
          val transferPair = v.getTransferPair(v.getAllocator)
          transferPair.transfer()
          val fv = transferPair.getTo.asInstanceOf[FieldVector]
          fv
        }

        foo(v)
      }).asJava
      new VectorSchemaRoot(sliceVectors)
    }
    new FrameBatch(new FrameSchema(transfer))
  }

  /**
   * copy a new FrameBatch, validityBuffer use bit store date mark validity value, valueBuffer use bytes store date
   * validityBuffer + valueBuffer = all allocated Buffer
   * len(validityBuffer) * 8 = buffer capacity, so max rowCount <= buffer capacity
   * len(valueBuffer) + len(validity) = pow(2,n)
   * rowCount * 8 <= len(valueBuffer)
   *
   * @param fb FrameBatch
   * @return new FrameBatch
   */
  def fork(fb: FrameBatch): FrameBatch = {
    val transfer: VectorSchemaRoot = {
      val sliceVectors = fb.rootSchema.arrowSchema.getFieldVectors.asScala.map((from: FieldVector) => {
        val field = from.getField
        val res = field.createVector(from.getAllocator)
        val rowCount = from.getValueCount
        res.setInitialCapacity(rowCount)
        res.allocateNew()
        res.setValueCount(rowCount)

        // copy by each
        for (i <- 0 until rowCount) {
          res.copyFrom(i, i, from) // use sun.misc.Unsafe
        }
        // copy by block
        //        val length = from.getDataBuffer.capacity() + from.getValidityBuffer.capacity()
        //        PlatformDependent.copyMemory(from.getDataBuffer.memoryAddress, res.getDataBuffer.memoryAddress, length - 8)
        //        for (i <- 0 until rowCount) {
        //          BitVectorHelper.setValidityBit(res.getValidityBuffer, i, 1)
        //        }

        res.asInstanceOf[FieldVector]
      }).asJava
      new VectorSchemaRoot(sliceVectors)
    }
    new FrameBatch(new FrameSchema(transfer))
  }

  /**
   * a fast way to copy data from heap memory to direct memory
   */
  def copyMemory(fv: FrameVector, value: Array[Double]): Unit = {
    val dataByteBuffer = fv.fieldVector.getDataBuffer.nioBuffer()
    // Order changed form little endian to big little after converting to ByteBuffer.So needing to set manually
    dataByteBuffer.order(ByteOrder.LITTLE_ENDIAN)
    dataByteBuffer.asDoubleBuffer().put(value)

    val validityByteBuffer = fv.fieldVector.getValidityBuffer
    val validityBits = Array.fill[Byte](validityByteBuffer.capacity().toInt)(-1)
    validityByteBuffer.setBytes(0, validityBits)
  }

  /**
   * convert direct memory to double array
   */
  def toDoubleArray(fv:FrameVector):Array[Double]= {
    val count = fv.valueCount
    val res = new Array[Double](count)
    res.indices.foreach{i =>
      res(i) = fv.readDouble(i)
    }
    res
  }

  /**
   * convert direct memory double type to float array
   */
  def toFloatArray(fv:FrameVector):Array[Float] = {
    val count = fv.valueCount
    val res = new Array[Float](count)
    res.indices.foreach{i =>
      res(i) = fv.readDouble(i).toFloat
    }
    res
  }

  val mockEmptyBatch:FrameBatch = {
    val schema = """{"fields": [{"name":"double0", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}]}"""
    new FrameBatch(new FrameSchema(schema), 0)
  }
}
