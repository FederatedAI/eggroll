package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.command.{CommandClient, CommandURI}
import com.webank.eggroll.core.constant.{MetadataCommands, StoreTypes}
import com.webank.eggroll.core.meta.{ErEndpoint, ErStore, ErStoreLocator}
import org.junit.Test

class TestSessionManager {

  @Test
  def testGetOrCreate():Unit = {
    println(TestAssets.sm1.getOrCreateSession(TestAssets.sessionMeta1))
  }
}
