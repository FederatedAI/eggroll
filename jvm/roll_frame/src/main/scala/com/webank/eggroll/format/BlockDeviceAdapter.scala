/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.webank.eggroll.format

import java.io._

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.session.StaticErConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.collection.concurrent.TrieMap


// need random access?
trait BlockDeviceAdapter {
  def getInputStream(): InputStream

  def getOutputStream(): OutputStream

  def close(): Unit = {}
}

object BlockDeviceAdapter {
  def apply(opts: Map[String, String]): BlockDeviceAdapter = {
    opts.getOrElse(StringConstants.TYPE, StringConstants.FILE) match {
      case StringConstants.CACHE =>
        new JvmBlockAdapter(opts(StringConstants.PATH), opts(StringConstants.SIZE).toInt)
      case StringConstants.HDFS =>
        new HdfsBlockAdapter(opts(StringConstants.PATH))
      case _ =>
        new FileBlockAdapter(opts(StringConstants.PATH))
    }
  }

  def file(path: String): BlockDeviceAdapter =
    apply(Map(StringConstants.PATH -> path, StringConstants.TYPE -> StringConstants.FILE))

  def hdfs(path: String): BlockDeviceAdapter =
    apply(Map(StringConstants.PATH -> path, StringConstants.TYPE -> StringConstants.HDFS))
}

class FileBlockAdapter(path: String) extends BlockDeviceAdapter {
  var inputStream: InputStream = _
  var outputStream: OutputStream = _

  def getInputStream(): InputStream = {
    inputStream = new FileInputStream(path)
    inputStream
  }

  override def getOutputStream(): OutputStream = {
    new File(path).getParentFile.mkdirs()
    outputStream = new FileOutputStream(path)
    outputStream
  }

  override def close(): Unit = {
    if (inputStream != null) {
      inputStream.close()
    }
    if (outputStream != null) {
      outputStream.close()
    }
  }
}

object HdfsBlockAdapter {
  /**
   * HDFS configuration get from properties file or user codes, example:
   * >> StaticErConf.addProperty("hadoop.fs.defaultFS","...")
   * >> StaticErConf.addProperty("hadoop.dfs.nameservices","...")
   * >> StaticErConf.addProperty("hadoop.dfs.namenode.rpc-address.nn1","...")
   * >> StaticErConf.addProperty("hadoop.dfs.namenode.rpc-address.nn2","...")
   */
  private var conf: Configuration = {
    //System.setProperty("HADOOP_USER_NAME", "hadoop")
    val defaultConf = new Configuration()
    val fsName = StaticErConf.getString("hadoop.fs.defaultFS", "")
    val fsNameServices = StaticErConf.getString("hadoop.dfs.nameservices", "")

    if (fsNameServices.nonEmpty) {
      // HA mode
      val nn1 = StaticErConf.getString("hadoop.dfs.namenode.rpc-address.nn1", "")
      val nn2 = StaticErConf.getString("hadoop.dfs.namenode.rpc-address.nn2", "")
      if (nn1.isEmpty) throw new NoSuchElementException("didn't set hadoop.dfs.namenode.rpc-address.nn1")
      if (nn2.isEmpty) throw new NoSuchElementException("didn't set hadoop.dfs.namenode.rpc-address.nn2")

      defaultConf.set("fs.defaultFS", fsName)
      defaultConf.set("dfs.nameservices", fsNameServices)
      defaultConf.set(s"dfs.ha.namenodes.${fsNameServices}", "nn1,nn2") // specific name
      defaultConf.set(s"dfs.namenode.rpc-address.${fsNameServices}.nn1", nn1)
      defaultConf.set(s"dfs.namenode.rpc-address.${fsNameServices}.nn2", nn2)
      defaultConf.set(s"dfs.client.failover.proxy.provider.${fsNameServices}", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    }
    else {
      // not HA model
      if (fsName.nonEmpty) {
        defaultConf.set("fs.defaultFS", fsName)
      } else {
        defaultConf.set("fs.defaultFS", "file://")
      }
    }
    defaultConf
  }

  /**
   * notice: hadoop conf can't set when use spark foreachPartition operation
   *
   * @param userConf conf
   */
  def setConfiguration(userConf: Configuration): Unit = {
    conf = userConf
  }
}

class HdfsBlockAdapter(path: String) extends BlockDeviceAdapter {
  var inputStream: InputStream = _
  var outputStream: OutputStream = _
  val hadoop: FileSystem = {
    FileSystem.get(HdfsBlockAdapter.conf)
  }

  override def getInputStream(): InputStream = {
    inputStream = hadoop.open(new Path(path))
    inputStream
  }

  override def getOutputStream(): OutputStream = {
    outputStream = hadoop.create(new Path(path))
    outputStream
  }

  override def close(): Unit = {
    if (inputStream != null) {
      inputStream.close()
    }
    if (outputStream != null) {
      outputStream.close()
    }
    if (hadoop != null) {
      hadoop.close()
    }
  }
}

class JvmBlockAdapter(path: String, size: Int) extends BlockDeviceAdapter {
  override def getInputStream(): InputStream = {
    // TODO: check null and size
    new ByteArrayInputStream(JvmBlockAdapter.get(path).get)
  }

  override def getOutputStream(): OutputStream = new ExternalBytesOutputStream(path, size)
}

class ExternalBytesOutputStream(path: String, size: Int) extends ByteArrayOutputStream(size) {
  override def close(): Unit = JvmBlockAdapter.put(path, buf)
}

object JvmBlockAdapter {
  private val data = new TrieMap[String, Array[Byte]]()

  def get(key: String): Option[Array[Byte]] = data.get(key)

  def put(key: String, bytes: Array[Byte]): Option[Array[Byte]] = data.put(key, bytes)
}

/*
class GrpcOutputStream(observer: StreamObserver[RollFrameGrpc.Batch],
                       batchSize: Int = 64 * 1024 * 1024) extends OutputStream {
  var batchWritten = 0
  var current: ByteString.Output = _
  val queue = new mutable.Queue[RollFrameGrpc.Batch]()

  override def write(b: Int): Unit = {
    if (current == null) {
      current = ByteString.newOutput(batchSize)
    }
    batchWritten += 4
    current.write(b)
    if (batchWritten >= batchSize) {
      writeBatch()
    }
  }

  private def writeBatch(): Unit = {
    observer.onNext(RollFrameGrpc.Batch.newBuilder().setData(current.toByteString).setId(
      RollFrameGrpc.BatchID.newBuilder().setSize(batchWritten)).build())
    current = null
    batchWritten = 0
  }

  override def close(): Unit = {
    // last one
    if (batchWritten > 0) {
      writeBatch()
    }
    observer.onCompleted()
  }
}

class GrpcInputStream(observer: StreamObserver[RollFrameGrpc.BatchID])
  extends InputStream with StreamObserver[RollFrameGrpc.Batch] {
  var queue: BlockingQueue[RollFrameGrpc.Batch] = new LinkedBlockingQueue[RollFrameGrpc.Batch]()
  var current: InputStream = _
  var end = false
  var blockEnd = false
  var lastId: RollFrameGrpc.BatchID = _

  override def read(): Int = {
    if (end) {
      return -1
    }
    if (current.available() <= 0) {
      current = queue.take().getData.newInput()
    }
    current.read()
  }


  override def onNext(value: RollFrameGrpc.Batch): Unit = {
    queue.put(value)
    lastId = value.getId
  }

  override def onError(t: Throwable): Unit = {
    t.printStackTrace()
  }

  override def onCompleted(): Unit = {
    observer.onNext(lastId)
    observer.onCompleted()
    end = true
  }
}
*/
