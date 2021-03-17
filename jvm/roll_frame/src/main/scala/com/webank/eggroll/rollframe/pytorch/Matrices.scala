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

package com.webank.eggroll.rollframe.pytorch

import com.webank.eggroll.format._
import com.webank.eggroll.util.SchemaUtil

class Matrices {

}

object Matrices {
  /**
   * matrix multiply vector
   *
   * @param cvs    ColumnVectors
   * @param vector Array[Double]
   * @return Array[Double]
   */
  def matMulVet(cvs: ColumnVectors, vector: Array[Double]): Array[Double] = {
    var res = Array[Double]()
    cvs.frames.foreach { cv =>
      val partRes = Torch.mm(cv.getDataBufferAddress, cv.valueCount, vector)
      res = res ++ partRes
    }
    res
  }

  /**
   * the core of matrix multiplying matrix
   *
   * @param cvs    ColumnVectors
   * @param matrix matrix
   * @param rows   rows
   * @param cols   cols
   * @return Array[Double]
   */
  private def matMulCore(cvs: ColumnVectors, matrix: Array[Double], rows: Int, cols: Int): Array[Double] = {
    var resData = Array[Double]()
    cvs.frames.foreach { cv =>
      val partRes = Torch.mm(cv.getDataBufferAddress, cv.valueCount, matrix, rows, cols)
      resData = resData ++ partRes
    }
    if (resData.length / cols != cvs.matrixRows) throw new IllegalStateException(s"The result' size of matrix mul is wrong")
    resData
  }

  /**
   * Matrix multiply matrix and return FrameBatch result
   *
   * @param cvs    ColumnVectors
   * @param matrix matrix
   * @param rows   rows of matrix
   * @param cols   cols of matrix
   * @return FrameBatch
   */
  def matMulToFbV1(cvs: ColumnVectors, matrix: Array[Double], rows: Int, cols: Int): FrameBatch = {
    var start = System.currentTimeMillis()
    val resData = matMulCore(cvs, matrix, rows, cols)
    println(s"Core mm time = ${System.currentTimeMillis() - start}")
    start = System.currentTimeMillis()
    val schema = SchemaUtil.getDoubleSchema(cols)
    val rootSchema = new FrameSchema(schema)
    val fb = new FrameBatch(rootSchema, cvs.matrixRows)
    // set value
    (0 until cols).foreach { i =>
      (0 until fb.rowCount).foreach { j =>
        fb.writeDouble(i, j, resData(i + j * cols))
      }
    }
    println(s"package fb time = ${System.currentTimeMillis() - start}")
    fb
  }

  def matMulToFb(cf: ColumnFrame, matrix: Array[Double], rows: Int, cols: Int): FrameBatch = {
    var start = System.currentTimeMillis()
    var resData = Array[Double]()
    val partRes = Torch.mm(cf.fb.rootVectors(0).getDataBufferAddress, cf.fb.rowCount, matrix, rows, cols)
    resData = resData ++ partRes

    println(s"Core mm time = ${System.currentTimeMillis() - start}")
    start = System.currentTimeMillis()
    val rootSchema = new FrameSchema(SchemaUtil.oneDoubleFieldSchema)
    val fb = new FrameBatch(rootSchema, cf.matrixRows*cols)
    FrameUtils.copyMemory(fb.rootVectors(0),resData)
    println(s"package fb time = ${System.currentTimeMillis() - start}")
    fb
  }

  /**
   * matrix multiply matrix and return two-dimensional array
   *
   * @param cvs    ColumnVectors
   * @param matrix matrix
   * @param rows   rows of matrix
   * @param cols   cols of matrix
   * @return
   */
  def matMulToArray(cvs: ColumnVectors, matrix: Array[Double], rows: Int, cols: Int, vertical: Boolean = false): Array[Array[Double]] = {
    var start = System.currentTimeMillis()
    val resData = matMulCore(cvs, matrix, rows, cols)
    println(s"inner mm time = ${System.currentTimeMillis() - start}")
    start = System.currentTimeMillis()
    // copy data, not best
    val newRows: Int = cvs.matrixRows

    val res = if (vertical) {
      val resMatrix = Array.ofDim[Double](cols, newRows)
      (0 until cols).foreach(i =>
        (0 until newRows).foreach(j => resMatrix(i)(j) = resData(i + j * cols))
      )
      resMatrix
    }
    else {
      val resMatrix = Array.ofDim[Double](newRows, cols)
      (0 until newRows).foreach(i =>
        (0 until cols).foreach(j => resMatrix(i)(j) = resData(i * cols + j)))
      resMatrix
    }
    println(s"package array time = ${System.currentTimeMillis() - start}")
    res
  }
}
