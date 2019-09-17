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

package com.webank.eggroll.rollframe

import com.webank.eggroll.blockdevice.BlockDeviceAdapter
import com.webank.eggroll.format._
import org.junit.Test

class FrameFormatTests {
  private val testAssets = TestAssets
  @Test
  def testNullableFields(): Unit = {
    val fb = new FrameBatch(new FrameSchema(testAssets.getDoubleSchema(4)), 3000)
    val path = "./tmp/unittests/RollFrameTests/filedb/test1/nullable_test"
    val adapter = FrameDB.file(path)
    adapter.writeAll(Iterator(fb.sliceByColumn(0,3)))
    adapter.close()
    val adapter2 = FrameDB.file(path)
    val fb2 = adapter2.readOne()
    assert(fb2.rowCount == 3000)
  }

  @Test
  def testFrameDataType():Unit = {
    val schema =
      """
      {
        "fields": [
          {"name":"double1", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}},
          {"name":"long1", "type": {"name" : "int","bitWidth" : 64,"isSigned":true}},
          {"name":"longarray1", "type": {"name" : "fixedsizelist","listSize" : 10},
            "children":[{"name":"$data$", "type": {"name" : "int","bitWidth" : 64,"isSigned":true}}]
          },
          {"name":"longlist1", "type": {"name" : "list"},
             "children":[{"name":"$data$", "type": {"name" : "int","bitWidth" : 64,"isSigned":true}}]
           }
        ]
      }
      """.stripMargin

    val batch = new FrameBatch(new FrameSchema(schema), 2)
    batch.writeDouble(0, 0, 1.2)
    batch.writeLong(1, 0, 22)
    val arr = batch.getArray(2, 0)
    val list0 = batch.getList(3, 0, 3)
    val list2 = batch.getList(3, 1, 4)
    batch.getList(3, 2, 5)
    val list0Copy = batch.getList(3, 0)
    //    list.valueCount(3)
    list0.writeLong(2, 33)
    list0.writeLong(0, 44)
    list2.writeLong(3, 55)
    (0 until 10).foreach( i=> arr.writeLong(i, i * 100 + 1))
    val outputStore = FrameDB.file("./tmp/unittests/RollFrameTests/filedb/test1/type_test")

    outputStore.append(batch)
    outputStore.close()
    assert(batch.readDouble(0, 0) == 1.2)
    assert(batch.readLong(1, 0) == 22)
    assert(arr.readLong(0) == 1)
    assert(list0.readLong(2) == 33)
    assert(list0.readLong(0) == 44)
    assert(list0Copy.readLong(0) == 44)

    val inputStore = FrameDB.file("./tmp/unittests/RollFrameTests/filedb/test1/type_test")
    for(b <- inputStore.readAll()) {
      assert(b.readDouble(0, 0) == 1.2)
      assert(b.readLong(1, 0) == 22)
      assert(b.getArray(2,0).readLong(0) == 1)
      assert(b.getList(3,0).readLong(0) == 44)
      assert(b.getList(3,0).readLong(2) == 33)
      assert(b.getList(3,1).readLong(3) == 55)

    }
  }

  @Test
  def testReadWrite():Unit = {
    val schema =
      """
        {
          "fields": [
          {"name":"double1", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}},
          {"name":"double2", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}},
          {"name":"double3", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}
          ]
        }
      """.stripMargin
    val path = "./tmp/unittests/RollFrameTests/testColumnarWrite/0"
    val cw = new FrameWriter(new FrameSchema(schema), BlockDeviceAdapter.file(path))
    val valueCount = 10
    val fieldCount = 3
    val batchSize = 5
    cw.write(valueCount, batchSize,
      (fid, cv) => (0 until valueCount).foreach(
        n => cv.writeDouble(n, fid * valueCount + n * 0.5)
      )
    )
    cw.close()
    val cr = new FrameReader(path)
    for( cb <- cr.getColumnarBatches()){
      for( fid <- 0 until fieldCount){
        val cv = cb.rootVectors(fid)
        for(n <- 0 until cv.valueCount) {
          assert(cv.readDouble(n) == fid * valueCount + n * 0.5)
        }
      }
    }
    cr.close()
  }
}
