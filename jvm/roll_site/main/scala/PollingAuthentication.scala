import java.io.IOException
import java.util
import java.util.Base64

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.PostMethod
import org.apache.commons.httpclient.methods.RequestEntity
import org.apache.commons.httpclient.methods.StringRequestEntity
import org.apache.commons.httpclient.params.HttpMethodParams

class PollingAuthentication {

  @throws[Exception]
  private def HmacSHA1Encrypt(encryptText: String, encryptKey: String) = {
    val data = encryptKey.getBytes("UTF-8")
    val secretKey = new SecretKeySpec(data, "HmacSHA1")
    val mac = Mac.getInstance("HmacSHA1")
    mac.init(secretKey)
    val text = encryptText.getBytes("UTF-8")
    mac.doFinal(text)
  }

  def generateSignature(partyId: String, role: String, appKey: String, timestamp: String, nonce: String, appSecret: String, uri: String, body: String): String = {
    try {
      var encryptText = partyId + "\n" + role + "\n" + appKey + "\n" + timestamp + "\n" + nonce + "\n" + uri + "\n" + body
      encryptText = new String(encryptText.getBytes, "UTF-8")
      return Base64.getEncoder.encodeToString(this.HmacSHA1Encrypt(encryptText, appSecret))
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    null
  }

  @throws[IOException]
  def sendPost(urlParam: String, heads: util.Map[String, String], paramJson: String): String = { // 创建httpClient实例对象
    // create httpClient
    val httpClient = new HttpClient
    // set httpClient connection timeout: 15000
    httpClient.getHttpConnectionManager.getParams.setConnectionTimeout(15000)
    val postMethod = new PostMethod(urlParam)
    postMethod.getParams.setParameter(HttpMethodParams.SO_TIMEOUT, 60000)
    postMethod.addRequestHeader("Content-Type", "application/json")
    // set post header
    import scala.collection.JavaConversions._
    for (entry <- heads.entrySet) {
      postMethod.addRequestHeader(entry.getKey, entry.getValue)
    }

    //parse json
    val entity: RequestEntity = new StringRequestEntity(paramJson, "application/json", "UTF-8")
    postMethod.setRequestEntity(entity)
    httpClient.executeMethod(postMethod)

    val result = postMethod.getResponseBodyAsString
    postMethod.releaseConnection()
    result
  }
}
