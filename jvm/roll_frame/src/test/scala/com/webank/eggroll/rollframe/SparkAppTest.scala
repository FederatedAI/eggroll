package com.webank.eggroll.rollframe

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.ErStore
import com.webank.eggroll.format.{FrameBatch, FrameSchema, FrameStore}
import junit.framework.TestCase
import org.junit.{Before, Test}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.TaskContext

class SparkAppTest extends Serializable {
  protected val ta = TestAssets
  protected val supportTorch = false
  protected var inputStore: ErStore = _
  protected val partitions_ = 3
  protected val rootPath = "/tmp/unittests/RollFrameTests/"
  val spark: SparkSession = SparkSession
    .builder()
    .appName("frame-debug")
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()
  println(s"Spark version: ${spark.version}")
  val df: Dataset[Row] = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
    .load("jvm/roll_frame/src/test/resources/data_1w_10cols.csv").repartition(3)

  @Test
  def testRddToRollFrame(): Unit = {
    val cols = df.columns.length
    val ctx: RollFrameContext = ta.getRfContext(true)
    val namespace = "test1"
    val name = "spark1"
    val storeType = StringConstants.HDFS
    val path = String.join(StringConstants.SLASH, rootPath, storeType, namespace, name)
    val store = ctx.createStore(namespace, name, storeType, partitions_)
    // TODO: spark rdd and rollframe FrameBatch have different store order
    val start = System.currentTimeMillis()
    df.rdd.foreachPartition { pData =>
      val data = pData.toList
      val fb = new FrameBatch(new FrameSchema(ta.getSchema(cols)))
      (0 until cols).foreach { field =>
        data.indices.foreach { row =>
          fb.writeDouble(field, row, data(row).get(field).toString.toDouble)
        }
      }
      val adapter = FrameStore.hdfs(path + TaskContext.getPartitionId())
      // TODO: store can't serialize, so it can't use NetWorkStore, only HdfsStore
      adapter.append(fb)
      adapter.close()
    }
    val end = System.currentTimeMillis()
    println(s"RddToRollFrame Time: ${end - start} ms")
  }

  @Test
  def testRddToRollFrameByCols(): Unit = {
    val cols = df.columns.length
    val ctx: RollFrameContext = ta.getRfContext(true)
    val namespace = "test1"
    val name = "spark1"
    val storeType = StringConstants.HDFS
    val path = String.join(StringConstants.SLASH, rootPath, storeType, namespace, name)
    val store = ctx.createStore(namespace, name, storeType, partitions_)
    val start = System.currentTimeMillis()
    df.rdd.foreachPartition { pData =>
      //      val data = pData.toList
      val fb = new FrameBatch(new FrameSchema(ta.getSchema(cols)))
      var i = 0
      pData.foreach { rows =>
        (0 until cols).foreach { field =>
          fb.writeDouble(field, i, rows.get(field).toString.toDouble)
        }
        i += 1
      }

      val adapter = FrameStore.hdfs(path + TaskContext.getPartitionId())
      adapter.append(fb)
      adapter.close()
    }
    val end = System.currentTimeMillis()
    println(s"RddToRollFrame Time: ${end - start} ms")
  }
}
