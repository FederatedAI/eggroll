package com.webank.eggroll.rollframe

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.{ErPartition, ErStore, ErStoreLocator}
import com.webank.eggroll.format.{FrameBatch, FrameDB, FrameSchema}

import scala.util.Random


object RfServerLauncher {
  private val ta = TestAssets
  private val ctx = ta.getRfContext()

  val storeLocator: ErStoreLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = StringConstants.HDFS)
  val input: ErStore = ErStore(storeLocator = storeLocator,
    partitions = Array(
      ErPartition(id = 0, storeLocator = storeLocator, processor = ta.clusterNode0),
      ErPartition(id = 1, storeLocator = storeLocator, processor = ta.clusterNode1),
      ErPartition(id = 2, storeLocator = storeLocator, processor = ta.clusterNode2)))


  def clientTask(name: String): Unit = {
    name match {
      case "c" => createHdfsData()
      case "v1" => verifyHdfsToJvm()
      case "v2" => verifyNetworkToJvm()
      case _ => throw new UnsupportedOperationException("not such task...")
    }
  }

  def verifyHdfsToJvm(): Unit = {
    var start = System.currentTimeMillis()
    val cacheStore = ta.loadCache(input)
    println("finish jvm to hdfs, time: " + (System.currentTimeMillis() - start))


    val outStoreLocator = ErStoreLocator(name = "v1", namespace = "test1", storeType = StringConstants.HDFS)
    val output = input.copy(storeLocator = outStoreLocator, partitions = input.partitions.map(p =>
      p.copy(storeLocator = outStoreLocator)))

    println("begin run jvm to hdfs")
    start = System.currentTimeMillis()
    val rf1 = ctx.load(cacheStore)
    rf1.mapBatch(cb => cb, output = output)
    println("finish jvm to hdfs, time: " + (System.currentTimeMillis() - start))
    // check hdfs whether has "v1" store
  }

  def verifyNetworkToJvm(): Unit = {
    val fbs = (0 until 3).map(i => FrameDB(input, i).readAll())

    val networkLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = StringConstants.NETWORK)
    val networkStore = input.copy(storeLocator = networkLocator, partitions = input.partitions.map(p =>
      p.copy(storeLocator = networkLocator)))

    var start = System.currentTimeMillis()
    fbs.indices.foreach(i => FrameDB(networkStore, i).writeAll(fbs(i)))
    println("finish fbs to network, time: " + (System.currentTimeMillis() - start))

    start = System.currentTimeMillis()
    val cacheStore = ta.loadCache(networkStore)
    println("finish network to jvm, time: " + (System.currentTimeMillis() - start))

    val outStoreLocator = ErStoreLocator(name = "v2", namespace = "test1", storeType = StringConstants.HDFS)
    val output = input.copy(storeLocator = outStoreLocator, partitions = input.partitions.map(p =>
      p.copy(storeLocator = outStoreLocator)))

    start = System.currentTimeMillis()
    val rf1 = ctx.load(cacheStore)
    rf1.mapBatch(cb => cb, output = output)
    println("finish jvm to hdfs, time: " + (System.currentTimeMillis() - start))
    // check hdfs whether has "v1" store
  }

  def createHdfsData(): Unit = {
    val fieldCount = 10
    val rowCount = 100 // total value count = rowCount * fbCount * fieldCount
    val fbCount = 1 // the num of batch

    def write(adapter: FrameDB): Unit = {
      val randomObj = new Random()
      (0 until fbCount).foreach { i =>
        val fb = new FrameBatch(new FrameSchema(getSchema(fieldCount)), rowCount)
        for {x <- 0 until fieldCount
             y <- 0 until rowCount} {
          fb.writeDouble(x, y, randomObj.nextDouble())
        }
        println(s"FrameBatch order: $i,row count: ${fb.rowCount}")
        adapter.append(fb)
      }
      adapter.close()
    }

    def read(adapter: FrameDB): Unit = {
      var num = 0
      adapter.readAll().foreach(_ => num += 1)
      val oneFb = adapter.readOne()
      adapter.close()
      assert(fbCount == num)
      assert(fieldCount == oneFb.fieldCount)
      assert(rowCount == oneFb.rowCount)
    }

    val output = ErStore(storeLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = StringConstants.HDFS))

    (0 until 3).foreach{ i =>
      write(FrameDB(output, i))
      read(FrameDB(output, i))
    }
  }


  private def getSchema(fieldCount: Int): String = {
    val sb = new StringBuilder
    sb.append("""{"fields": [""")
    (0 until fieldCount).foreach { i =>
      if (i > 0) {
        sb.append(",")
      }
      sb.append(s"""{"name":"double$i", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}""")
    }
    sb.append("]}")
    sb.toString()
  }
}