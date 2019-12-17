package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta}
import com.webank.eggroll.core.resourcemanager.ResourceDao.NotExistError
import com.webank.eggroll.core.session.StaticErConf
import org.junit.Test
import org.junit.Assert._

import scala.collection.JavaConverters._
class TestResourceDao {
  StaticErConf.addProperties("./jvm/core/main/resources/cluster-manager.properties")
  private val smDao = new SessionMetaDao
  private val proc1 = ErProcessor(serverNodeId = 1, status = RmConst.PROC_NEW)
  private val session = ErSessionMeta(
    id="sid_reg1", tag = "tag1",
    options = Map("a"->"b","c"->"d"), processors = List(proc1))
  @Test
  def testRegisterSession():Unit = {
    smDao.register(session)
    println(smDao.getSession(session.id))
    var expect:Throwable = null
    try {
      smDao.addSession(session)
    } catch {
      case e: NotExistError =>
        expect = e
        println("got it:" + e.getMessage)
    }
    assert(expect.isInstanceOf[IllegalArgumentException])
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testGetSessionError():Unit = {
    smDao.getSession("-999")
  }

  @Test
  def testUpdateSessionProcessor():Unit = {
    smDao.register(session)
    val sessRet = smDao.getSession(session.id)
    val procRet = sessRet.processors.head
    assertEquals(0, smDao.getSessionMain(session.id).activeProcCount)
    smDao.updateProcessor(procRet.copy(status = RmConst.PROC_READY))
    smDao.updateProcessor(procRet.copy(status = RmConst.PROC_READY))
    assertEquals(1, smDao.getSessionMain(session.id).activeProcCount)
    smDao.updateProcessor(procRet.copy(status = RmConst.PROC_NEW))
    assertEquals(0, smDao.getSessionMain(session.id).activeProcCount)
    smDao.updateProcessor(procRet.copy(status = RmConst.PROC_NEW))
    assertEquals(0, smDao.getSessionMain(session.id).activeProcCount)

  }
}
