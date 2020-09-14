package com.webank.eggroll.rollframe

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.ErStore
import com.webank.eggroll.format.{FrameBatch, FrameSchema, FrameStore}
import com.webank.eggroll.util.SchemaUtil
import junit.framework.TestCase
import org.junit.{Before, Test}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

class SparkAppTest extends Serializable {
  protected val ta = TestAssets
  protected val supportTorch = false
  protected var inputStore: ErStore = _
  protected val partitions_ = 8
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
    val ctx = ta.getRfContext()
    val namespace = "test1"
    val name = "dataframe"
    val storeType = StringConstants.NETWORK
    val networkStore = ctx.createStore(namespace, name, storeType, partitions_)
    val partitionsMata = FrameStore.getPartitionsMeta(networkStore)
    val start = System.currentTimeMillis()
    df.rdd.foreachPartition { pData =>
      val data = pData.toArray
      val columns = data(0).length
      val rowCount = data.length
      val fb = new FrameBatch(new FrameSchema(ta.getSchema(cols)), rowCount)
      (0 until columns).foreach { f =>
        (0 until rowCount).foreach { r =>
          fb.writeDouble(f, r, data(r).get(f).toString.toDouble)
        }
      }
      val partitionMeta = partitionsMata(TaskContext.getPartitionId())
      val adapter = FrameStore.network(partitionMeta("path"), partitionMeta("host"), partitionMeta("port"))
      adapter.append(fb)
      adapter.close()
    }
    // to cache
    println("\n ======= to Cache =======\n")
    val cacheStore = ctx.dumpCache(networkStore)
    val end = System.currentTimeMillis()
    println(s"RddToRollFrame Time: ${end - start} ms")
    ctx.frameTransfer.releaseStore()
    Thread.sleep(1000)
    println("done")
  }

  @Test
  def testRddToRollFrame1(): Unit = {
    val ctx = ta.getRfContext()
    val namespace = "test1"
    val name = "dataframe"
    val storeType = StringConstants.NETWORK
    val networkStore = ctx.createStore(namespace, name, storeType, partitions_)
    val partitionsMata = FrameStore.getPartitionsMeta(networkStore)
    val start = System.currentTimeMillis()
    df.rdd.foreachPartition { pData =>
      val (iterator1, iterator2) = pData.duplicate
      val rowCount = iterator1.length
      val fb = new FrameBatch(new FrameSchema(ta.getSchema(cols)), rowCount)
      var i = 0
      iterator2.foreach { rows =>
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
    println("\n ======= to Cache =======\n")
    val cacheStore = ctx.dumpCache(networkStore)
    val end = System.currentTimeMillis()
    println(s"RddToRollFrame Time: ${end - start} ms")
    ctx.load(cacheStore).mapBatch({ fb =>
      TestCase.assertEquals(fb.fieldCount, cols)
      fb
    })
  }

  @Test
  def testRddToRollFrame2(): Unit = {
    val ctx = ta.getRfContext(true)
    val namespace = "test1"
    val name = "dataframe"
    val storeType = StringConstants.NETWORK
    val networkStore = ctx.createStore(namespace, name, storeType, partitions_)
    val partitionsMata = FrameStore.getPartitionsMeta(networkStore)
    val start = System.currentTimeMillis()

    df.rdd.foreachPartition { pData =>
      val (iterator1, iterator2) = pData.duplicate
      var it: Iterator[Row] = iterator2
      val rowCount = iterator1.length
      val fb = new FrameBatch(new FrameSchema(ta.getSchema(cols)), rowCount)

      (0 until cols).foreach { field =>
        var i = 0
        val (it1, it2) = it.duplicate
        it1.foreach { row =>
          i += 1
          fb.writeDouble(field, i, row.get(field).toString.toDouble)
        }
        it = it2
      }
      val partitionMeta = partitionsMata(TaskContext.getPartitionId())
      val adapter = FrameStore.network(partitionMeta("path"), partitionMeta("host"), partitionMeta("port"))
      adapter.append(fb)
      adapter.close()
    }
    // to cache
    println("\n ======= to Cache =======\n")
    val cacheStore = ctx.dumpCache(networkStore)
    val end = System.currentTimeMillis()
    println(s"RddToRollFrame Time: ${end - start} ms")
    ctx.load(cacheStore).mapBatch({ fb =>
      TestCase.assertEquals(fb.fieldCount, cols)
      fb
    })
  }

  @Test
  def testRddToRollFrame3(): Unit = {
    df.cache()
    df.first()
    val ctx = ta.getRfContext()
    val namespace = "test1"
    val name = "dataframe"
    val storeType = StringConstants.NETWORK
    //    val numPartitions = df.rdd.getNumPartitions
    val networkStore = ctx.createStore(namespace, name, storeType, partitions_, singleThread = true)
    val partitionsMata = FrameStore.getPartitionsMeta(networkStore)
    var start = System.currentTimeMillis()

    df.rdd.foreachPartition { pData =>
      val data = pData.toArray
      val columns = data(0).length
      val rowCount = data.length
      val fb = new FrameBatch(new FrameSchema(ta.getSchema(cols)), rowCount)
      (0 until columns).foreach { f =>
        (0 until rowCount).foreach { r =>
          fb.writeDouble(f, r, data(r).get(f).toString.toDouble)
        }
      }
      val partitionMeta = partitionsMata(TaskContext.getPartitionId())
      val adapter = FrameStore.network(partitionMeta("path"), partitionMeta("host"), partitionMeta("port"))
      adapter.append(fb)
      adapter.close()
    }
    // to cache
    println("\n ======= to Cache =======\n")
    val cacheStore = ctx.dumpCache(networkStore)
    println(s"RddToRollFrame Time: ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    println(FrameStore.getStorePath(cacheStore.partitions(0)))
    val combineStore = ctx.combineDoubleFbs(cacheStore)
    println(s"combine Time: ${System.currentTimeMillis() - start} ms")
    ctx.frameTransfer.releaseStore()
    Thread.sleep(1000)
    println("done")
  }

  @Test
  def testRddToRollFrame4(): Unit = {
    //    df.cache()
    //    df.count()
    val ctx = ta.getRfContext()
    val namespace = "test1"
    val name = "dataframe"
    val storeType = StringConstants.NETWORK
    //    val numPartitions = df.rdd.getNumPartitions
    val networkStore = ctx.createStore(namespace, name, storeType, partitions_)
    val partitionsMata = FrameStore.getPartitionsMeta(networkStore)
    var start = System.currentTimeMillis()
    df.rdd.foreachPartition { pData =>
      val data = pData.toArray
      val columns = data(0).length
      val rowCount = data.length
      val fb = new FrameBatch(new FrameSchema(FrameSchema.oneFieldSchema), columns * rowCount)
      (0 until rowCount).foreach { r =>
        (0 until columns).foreach { f =>
          val index = r * columns + f
          fb.writeDouble(0, index, data(r).get(f).toString.toDouble)
        }
      }
      val partitionMeta = partitionsMata(TaskContext.getPartitionId())
      val adapter = FrameStore.network(partitionMeta("path"), partitionMeta("host"), partitionMeta("port"))
      adapter.append(fb)
      adapter.close()
    }
    // to cache
    println("\n ======= to Cache =======\n")
    val cacheStore = ctx.dumpCache(networkStore)
    println(s"RddToRollFrame Time: ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    println(FrameStore.getStorePath(cacheStore.partitions(0)))
    val combineStore = ctx.combineDoubleFbs(cacheStore)
    println(s"combine Time: ${System.currentTimeMillis() - start} ms")
    ctx.frameTransfer.releaseStore()
    Thread.sleep(1000)
    println("done")
  }
}
