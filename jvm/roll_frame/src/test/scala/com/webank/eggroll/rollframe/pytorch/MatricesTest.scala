package com.webank.eggroll.rollframe.pytorch

import com.webank.eggroll.format.{ColumnVectors, FrameBatch, FrameSchema}
import com.webank.eggroll.rollframe.TestAssets
import com.webank.eggroll.util.SchemaUtil
import junit.framework.TestCase
import org.junit.{Before, Test}

@Deprecated
class MatricesTest {

  @Before
  def loadLibrary(): Unit = {
    LibraryLoader.load
        println(System.getProperty("java.library.path"))
  }

  @Test
  def primaryDotTest(): Unit = {
    val v1 = Array(1f, -1f, 1f)
    val v2 = Array(1f, 1f, 1f)
    val res = Torch.primaryDot(v1, v2)
    TestCase.assertEquals(res, 1.0, TestAssets.DELTA)
  }

  @Test
  def matrixMulVectorTest(): Unit = {
    val rows = 10000
    val columns = 500
    val columnVectors = createColumnVectors(rows, columns, 1)
    val vector = Array.fill[Double](columns)(1)
    val start = System.currentTimeMillis()
    val res = Matrices.matMulVet(columnVectors, vector)
    println(s"mul time:${System.currentTimeMillis() - start}")
    TestCase.assertEquals(res.length, rows)
    TestCase.assertEquals(res(0), columns, TestAssets.DELTA)
  }


  @Test
  def matrixMulMatrixToFbV1Test(): Unit = {
    val lRows = 10000
    val lColumns = 1000
    val rRows = 1000
    val rColumns = 1000
    var start = System.currentTimeMillis()
    val columnVectors = createColumnVectors(lRows, lColumns, 1)
    println(s"create cv time = ${System.currentTimeMillis() - start}")
    val matrix = Array.fill[Double](rRows * rColumns)(1.0)
    start = System.currentTimeMillis()
    val resFb = Matrices.matMulToFbV1(columnVectors, matrix, rRows, rColumns)
    println(s"mul time = ${System.currentTimeMillis() - start}")
    TestCase.assertEquals(resFb.rowCount, lRows)
    TestCase.assertEquals(resFb.fieldCount, rColumns)
    TestCase.assertEquals(resFb.readDouble(0, 0), lColumns, TestAssets.DELTA)
  }

  @Test
  def matrixMulMatrixToArrayTest(): Unit = {
    val lRows = 10000
    val lColumns = 1000
    val rRows = 1000
    val rColumns = 100
    var start = System.currentTimeMillis()
    val columnVectors = createColumnVectors(lRows, lColumns, 1)
    println(s"create cv time = ${System.currentTimeMillis() - start}")
    start = System.currentTimeMillis()
    val matrix = Array.fill[Double](rRows * rColumns)(1.0)
    println(s"create matrix time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    val res = Matrices.matMulToArray(columnVectors, matrix, rRows, rColumns)
    println(s"mul time = ${System.currentTimeMillis() - start}")
    TestCase.assertEquals(res.length, lRows)
    TestCase.assertEquals(res(0).length, rColumns)
    TestCase.assertEquals(res(0)(0), lColumns, TestAssets.DELTA)

    start = System.currentTimeMillis()
    val res1 = Matrices.matMulToArray(columnVectors, matrix, rRows, rColumns, vertical = true)
    println(s"mul1 time = ${System.currentTimeMillis() - start}")
    TestCase.assertEquals(res1.length, rColumns)
    TestCase.assertEquals(res1(0).length, lRows)
    TestCase.assertEquals(res1(0)(0), lColumns, TestAssets.DELTA)
  }

  def createColumnVectors(rows: Int, columns: Int, value: Double): ColumnVectors = {
    val schema = SchemaUtil.getDoubleSchema(columns)
    var start = System.currentTimeMillis()
    val rootSchema = new FrameSchema(schema)
    println(s"create rootSchema time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    val fb = new FrameBatch(rootSchema, rows)
    println(s"new FrameBatch time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    for {x <- 0 until columns
         y <- 0 until rows} {
      fb.writeDouble(x, y, value)
    }
    println(s"set values time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    val cv = fb.toColumnVectors
    println(s"to ColumnVector Time = ${System.currentTimeMillis() - start} ms")
    cv
  }
}
