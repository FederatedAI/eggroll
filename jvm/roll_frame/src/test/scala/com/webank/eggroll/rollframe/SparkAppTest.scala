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
  protected val spark: SparkSession = SparkSession
    .builder()
    .appName("frame-debug")
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()
  println(s"Spark version: ${spark.version}")
  protected val df: Dataset[Row] = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
    .load("jvm/roll_frame/src/test/resources/data_1w_10cols.csv").repartition(partitions_)
  val cols: Int = df.columns.length


  @Test
  def testRddToRollFrame(): Unit = {
    val ctx = ta.getRfContext(true)
    val namespace = "test1"
    val name = "dataframe"
    val storeType = StringConstants.NETWORK
    val networkStore = ctx.createStore(namespace, name, storeType, partitions_)
    val partitionsMata = FrameStore.getPartitionsMeta(networkStore)
    val start = System.currentTimeMillis()
    df.rdd.foreachPartition { pData =>
      val fb = new FrameBatch(new FrameSchema(ta.getSchema(cols)), pData.size)
      var i = 0
      pData.foreach { rows =>
        (0 until cols).foreach { field =>
          fb.writeDouble(field, i, rows.get(field).toString.toDouble)
        }
        i += 1
      }
      val partitionMeta = partitionsMata(TaskContext.getPartitionId())
      val adapter = FrameStore.network(partitionMeta("path"), partitionMeta("host"), partitionMeta("port"))
      adapter.append(fb)
      adapter.close()
    }
    // to cache
    val cacheStore = ctx.dumpCache(networkStore)
    val end = System.currentTimeMillis()
    println(s"RddToRollFrame Time: ${end - start} ms")

    ctx.load(cacheStore).mapBatch({ fb =>
      TestCase.assertEquals(fb.fieldCount, cols)
      fb
    })
  }
}
