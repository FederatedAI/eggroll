package com.webank.eggroll.rollframe
import junit.framework.TestCase
import org.junit.{Before, Test}
import org.apache.spark.sql.SparkSession
class SparkAppTest {
  @Test
  def testRddToRollFrame(): Unit ={
    // TODO: unfinished
    val ss = SparkSession.builder().appName("frame-debug").enableHiveSupport().master("local[*]").getOrCreate()
    println(ss.version)
  }


}
