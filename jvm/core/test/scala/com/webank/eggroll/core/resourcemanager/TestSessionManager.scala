package com.webank.eggroll.core.resourcemanager

import com.google.gson.Gson
import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.deepspeed.job.meta.{DownloadJobRequest, PrepareJobDownloadRequest}
import com.webank.eggroll.core.transfer.ExtendTransferServerGrpc
import com.webank.eggroll.core.transfer.ExtendTransferServerGrpc.ExtendTransferServerStub
import org.junit.Test

class TestSessionManager {

  @Test
  def testSubmitJob(): Unit = {
    val clusterManagerClient = new ClusterManagerClient("localhost", 4670)
    val result = clusterManagerClient.submitJob(job = TestAssets.submitJobMeta)
    println(result)
  }
  @Test
  def  testDownloadV2(): Unit = {
    var gson :Gson = new Gson()
    val clusterManagerClient = new ClusterManagerClient("localhost", 4670)
    var result = clusterManagerClient.prepareJobDownload(job = PrepareJobDownloadRequest(sessionId = "deepspeed_session_20230705-175508-766715",ranks= new Array[Int](0),
      compressMethod="",contentType =com.webank.eggroll.core.containers.meta.ContentType.ALL ))
    var  prepareData :java.util.Map[String,String]=gson.fromJson(result.content,classOf[java.util.Map[String,String]])


    System.err.println(prepareData)
  }

  @Test
  def testGetOrCreate():Unit = {


      new Thread(()=>{
        val clusterManagerClient = new ClusterManagerClient("localhost", 4670)
        val result = clusterManagerClient.getOrCreateSession(sessionMeta = TestAssets.createNewGetOrCreateSessionMeta)
        println(result)
      }).start()

    val clusterManagerClient = new ClusterManagerClient("localhost", 4670)
    val result = clusterManagerClient.getOrCreateSession(sessionMeta = TestAssets.createNewGetOrCreateSessionMeta)
    println(result)

  }

  @Test
  def testStop(): Unit = {
    val clusterManagerClient = new ClusterManagerClient("localhost",4670)
    val result = clusterManagerClient.stopSession(sessionMeta = TestAssets.getOrCreateSessionMeta)
  }
}
