package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.client.ClusterManagerClient
import org.junit.Test

class TestSessionManager {

  @Test
  def testGetOrCreate():Unit = {
    val clusterManagerClient = new ClusterManagerClient()
    val result = clusterManagerClient.getOrCreateSession(sessionMeta = TestAssets.getOrCreateSessionMeta)
    println(result)
  }

  @Test
  def testStop(): Unit = {
    val clusterManagerClient = new ClusterManagerClient()
    val result = clusterManagerClient.stopSession(sessionMeta = TestAssets.getOrCreateSessionMeta)
  }
}
