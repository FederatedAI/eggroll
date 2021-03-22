package com.webank.eggroll.rollsite

import java.util
import java.util.UUID

import com.webank.ai.eggroll.api.networking.proxy.Proxy
import com.webank.eggroll.core.constant.RollSiteConfKeys
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.reflect.MethodUtils
import org.json.JSONObject
import java.lang.reflect.Method

import javax.security.sasl.AuthenticationException
import org.apache.commons.lang3.StringUtils


trait PollingAuthenticator {
  def getSecretInfo(): JSONObject

  def sign(): String

  def authenticate(req: Proxy.PollingFrame): Boolean
}

object FatePollingAuthenticator extends Logging{
  private val clazz: Class[_] = Class.forName("com.webank.ai.fate.cloud.sdk.sdk.Fatecloud")
  private val fateCloud = clazz.newInstance()
  private val getSecretInfoMethod = MethodUtils.getMatchingMethod(clazz, "getSecretInfo", classOf[String], classOf[String])
  private val signMethod = MethodUtils.getMatchingAccessibleMethod(clazz, "generateSignature",
    classOf[String], classOf[String], classOf[String], classOf[String], classOf[String], classOf[String], classOf[String], classOf[String])
  private val authenticateMethod: Method = MethodUtils.getMatchingMethod(clazz, "checkPartyId",
    classOf[String], classOf[util.HashMap[String, String]], classOf[String])
}

class FatePollingAuthenticator extends PollingAuthenticator with Logging{
  def getSecretInfo(): JSONObject = {
    // generate signature
    val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get().toInt
    val secretInfoUrl = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_SECRET_INFO_URL.get().toString
    var appSecret: String = null
    var appKey: String = null
    var role: String = null
    if (RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_USE_CONFIG.get().toBoolean) {
      logTrace(s"manual configuration enabled, getting appKey, appSecret and party role from eggroll.properties")
      appKey = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_APPKEY.get().toString
      appSecret = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_APPSERCRET.get().toString
      if (appKey == null || appSecret == null) {
        throw new IllegalArgumentException(s"failed to get appKey or appSecret or party role from eggroll.properties")
      }
      RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_ROLE.get().toString.toLowerCase match {
        case "guest" => role = "1"
        case "host" => role = "2"
        case x => throw new AuthenticationException(s"unsupported role=${x}")
      }
      if (role == null) {
        throw new AuthenticationException(s"role is null, please check if eggroll.rollsite.polling.authentication.role has been configured")
      }
    } else {
      val args = secretInfoUrl + "," + myPartyId
      logTrace(s"getSecretInfo of fateCloud calling, args=${args}")
      val result = FatePollingAuthenticator.getSecretInfoMethod.invoke(FatePollingAuthenticator.fateCloud, secretInfoUrl, myPartyId.toString)
      logTrace(s"getSecretInfo of fateCloud called")
      if (result == null || StringUtils.isBlank(result.toString)) {
        throw new AuthenticationException(s"result of getSecretInfo is empty")
      }

      val secretInfo = new JSONObject(result.toString)
      if (secretInfo.get("data") == JSONObject.NULL) {
        logError(s"partyID:${myPartyId} not registered")
        throw new AuthenticationException(s"partyID=${myPartyId} is not registered")
      }

      appSecret = secretInfo.getJSONObject("data").getString("appSecret")
      appKey = secretInfo.getJSONObject("data").getString("appKey")
      logTrace(s"role of ${myPartyId} is ${secretInfo.getJSONObject("data").getString("role")}")
      role = if (secretInfo.getJSONObject("data").getString("role").toLowerCase() == "guest") "1" else "2"
    }
    val secretInfo: JSONObject = new JSONObject
    secretInfo.put("appSecret", appSecret)
    secretInfo.put("appKey", appKey)
    secretInfo.put("role", role)
    secretInfo
  }

  def sign(): String = {
    val secretInfoGen = getSecretInfo()
    val appSecret = secretInfoGen.get("appSecret").toString
    val appKey = secretInfoGen.get("appKey").toString
    val role = secretInfoGen.get("role").toString
    val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get().toString
    val time = String.valueOf(System.currentTimeMillis)
    val uuid = UUID.randomUUID.toString
    val nonce = uuid.replaceAll("-", "")
    val httpURI = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_URI.get().toString
    val body = ""
    val args: Array[String] = Array(myPartyId, role, appKey, time, nonce, httpURI, body)
    logTrace(s"generateSignature of fateCloud calling")
    val signature = FatePollingAuthenticator.signMethod.invoke(FatePollingAuthenticator.fateCloud, appSecret, args)
    logTrace(s"generateSignature of fateCloud called, signature=${signature}")

    if (signature == null) {
      throw new AuthenticationException(s"signature generated is null, please check args appSecret=${appSecret}, partyId=${myPartyId}, " +
        s"role=${role}, appKey=${appKey}, time=${time}, nonce=${nonce}, httpURI=${httpURI}, body=${body}")
    }

    val authInfo: JSONObject = new JSONObject
    authInfo.put("signature", signature.toString)
    authInfo.put("appKey", appKey.toString)
    authInfo.put("timestamp", time.toString)
    authInfo.put("nonce", nonce.toString)
    authInfo.put("role", role.toString)
    authInfo.put("httpUri", httpURI)
    logTrace(s"authInfo to be sent=${authInfo}")
    authInfo.toString
  }

  def authenticate(req: Proxy.PollingFrame): Boolean = {
    val authUrl = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_URL.get().toString
    val authString = req.getMetadata.getTask.getModel.getDataKey
    logTrace(s"req metaData recv=${req.getMetadata.toString}")
    if (authString == "" || authString == null) {
      throw new AuthenticationException(s"failed to get authentication info from header=${req.getMetadata.toString}")
    }
    val authInfo = new JSONObject(authString)

    val signature = authInfo.getString("signature")
    val appKey = authInfo.getString("appKey")
    val timestamp = authInfo.getString("timestamp")
    val nonce = authInfo.getString("nonce")
    val role = authInfo.getString("role")
    val httpUri = authInfo.getString("httpUri")
    val authPartyID = req.getMetadata.getDst.getPartyId

    val heads = new util.HashMap[String, String]()
    heads.put("TIMESTAMP", timestamp)
    heads.put("PARTY_ID", authPartyID)
    heads.put("NONCE", nonce)
    heads.put("ROLE", role)
    heads.put("APP_KEY", appKey)
    heads.put("URI", httpUri)
    heads.put("SIGNATURE", signature)
    val body = ""
    logTrace(s"auth heads:${heads.toString}")

    logTrace(s"checkPartyId of fateCloud calling")
    try {
      // val result = FatePollingAuthenticator.fateCloud.checkPartyId(authUrl, heads, body)
      val result = FatePollingAuthenticator.authenticateMethod.invoke(FatePollingAuthenticator.fateCloud, authUrl, heads, body).asInstanceOf[Boolean]
      logTrace(s"checkPartyId of fateCloud called")
      result
    } catch {
      case e: java.lang.reflect.InvocationTargetException =>
        if (e.getTargetException.isInstanceOf[java.net.ConnectException]) {
          logError(s"server authenticate failed to connect to ${authUrl}")
          throw new java.net.ConnectException(s"polling server authenticate failed to connect to authentication server")
        } else {
          throw new Exception(s"failed to authenticate, please check polling client authentication info=${authString}", e)
        }
      case a: AuthenticationException =>
        throw new AuthenticationException(s"failed to authenticate, please check polling client authentication info=${authString}", a)
      case t: Throwable =>
        throw new Exception(s"failed to authenticate, please check polling client authentication info=${authString}", t)
    }
  }
}
