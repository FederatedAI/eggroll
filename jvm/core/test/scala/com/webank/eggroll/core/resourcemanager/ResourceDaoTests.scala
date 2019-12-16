package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.meta.ErSessionMeta
import com.webank.eggroll.core.session.StaticErConf
import org.junit.Test

import scala.collection.JavaConverters._
class ResourceDaoTests {
  StaticErConf.addProperties("./jvm/core/main/resources/cluster-manager.properties")
  private val sessionMetaDao = new SessionMetaDao
  @Test
  def testRegisterSession():Unit = {
    val session = ErSessionMeta(id="sid_reg1", tag = "tag1",
      options = Map("a"->"b","c"->"d").toMap.asJava)
    sessionMetaDao.register(session)
    println(sessionMetaDao.getSession(session.id))
    var expect:Throwable = null
    try {
      sessionMetaDao.addSession(session)
    } catch {
      case e: IllegalArgumentException =>
        expect = e
        println("got it:" + e.getMessage)
    }
    assert(expect.isInstanceOf[IllegalArgumentException])
  }
}
