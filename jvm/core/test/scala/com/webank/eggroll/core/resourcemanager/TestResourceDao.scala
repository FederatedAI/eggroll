package com.webank.eggroll.core.resourcemanager

import java.io.File

import com.webank.eggroll.core.constant.ProcessorStatus
import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta}
import com.webank.eggroll.core.resourcemanager.ResourceDao.NotExistError
import com.webank.eggroll.core.session.StaticErConf
import org.junit.Assert._
import org.junit.Test
class TestResourceDao {
  private val confFile = new File("../../conf/eggroll.properties.local")
  println(confFile.getAbsolutePath)
  StaticErConf.addProperties(confFile.getAbsolutePath)
  private val smDao = new SessionMetaDao
  private val proc1 = ErProcessor(serverNodeId = 1, status = ProcessorStatus.NEW)
  private val session = ErSessionMeta(
    id="sid_reg1", tag = "tag1",
    options = Map("a"->"b","c"->"d"), processors = Array(proc1))
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
    smDao.updateProcessor(procRet.copy(status = ProcessorStatus.RUNNING))
    smDao.updateProcessor(procRet.copy(status = ProcessorStatus.RUNNING))
    assertEquals(1, smDao.getSessionMain(session.id).activeProcCount)
    smDao.updateProcessor(procRet.copy(status = ProcessorStatus.NEW))
    assertEquals(0, smDao.getSessionMain(session.id).activeProcCount)
    smDao.updateProcessor(procRet.copy(status = ProcessorStatus.NEW))
    assertEquals(0, smDao.getSessionMain(session.id).activeProcCount)

  }

  @Test
  def testCreateProcessor(): Unit = {
    smDao.createProcessor(proc1)
  }
}
