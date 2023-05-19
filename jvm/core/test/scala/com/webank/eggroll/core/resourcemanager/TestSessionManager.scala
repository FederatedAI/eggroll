package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.client.ClusterManagerClient
import org.junit.Test

class TestSessionManager {

  @Test
  def testSubmitJob(): Unit = {
    val clusterManagerClient = new ClusterManagerClient("localhost", 4670)
//    val result = clusterManagerClient.submitJob(job = TestAssets.submitJobMeta)
//    println(result)
  }


  @Test
  def testGetOrCreate():Unit = {
    val clusterManagerClient = new ClusterManagerClient("localhost",4670)
    val result = clusterManagerClient.getOrCreateSession(sessionMeta = TestAssets.getOrCreateSessionMeta)
    println(result)
  }

  @Test
  def testStop(): Unit = {
    val clusterManagerClient = new ClusterManagerClient("localhost",4670)
    val result = clusterManagerClient.stopSession(sessionMeta = TestAssets.getOrCreateSessionMeta)
  }
}
