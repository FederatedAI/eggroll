package com.webank.eggroll.rollsite

import java.util
import java.util.UUID

import com.webank.ai.eggroll.api.networking.proxy.Proxy
import com.webank.eggroll.core.constant.RollSiteConfKeys
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.reflect.MethodUtils
import org.json.JSONObject
import javax.security.sasl.AuthenticationException

class PollingAuthentication extends Logging{
  def genSecretInfo(): String = {
    // generate signature
    val myPartyId = RollSiteConfKeys.EGGROLL_ROLLSITE_PARTY_ID.get().toInt
    val secretInfoUrl = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_SECRET_INFO_URL.get().toString
    var appSecret = ""
    var appKey = ""
    var role = ""
    val authInfoSecretGenerator = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_SECRECT_INFO_GENERATOR.get().toString
    var splitted = authInfoSecretGenerator.split("#")
    val authenticator = Class.forName(splitted(0)).newInstance()

    if (RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_USE_CONFIGURE.get().toBoolean) {
      logDebug(s"manual configuration enabled, getting appKey, appSecret and party role from eggroll.properties")
      appKey = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_APPKEY.get().toString
      appSecret = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_APPSERCRET.get().toString
      if (appKey == null || appSecret == null) {
        throw new IllegalArgumentException(s"failed to get appKey or appSecret or party role from eggroll.properties")
      }
      role = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_ROLE.get().toString.toLowerCase match {
        case "guest" => "1"
        case "host" => "2"
        case x => throw new AuthenticationException(s"unsupported role=${x}")
      }
    } else {


      val args = secretInfoUrl + "," + myPartyId
      logDebug(s"${splitted(1)} of ${splitted(0)} calling, args=${args}")
      val result = MethodUtils.invokeExactMethod(authenticator, splitted(1), args.split(","): _*).asInstanceOf[String]
      logDebug(s"${splitted(1)} of ${splitted(0)} called")
      if (result == null || result == "") {
        throw new AuthenticationException(s"result of ${splitted(1)} is empty")
      }

      val secretInfo = new JSONObject(result.mkString)
      if (secretInfo.getJSONObject("data") == null) {
        logError(s"partyID:${myPartyId} not registered")
        throw new AuthenticationException(s"partyID:${myPartyId} not registered")
      }

      appSecret = secretInfo.getJSONObject("data").getString("appSecret")
      appKey = secretInfo.getJSONObject("data").getString("appKey")
      logTrace(s"role of ${myPartyId} is ${secretInfo.getJSONObject("data").getString("role")}")
      role = if (secretInfo.getJSONObject("data").getString("role").toLowerCase() == "guest") "1" else "2"
    }


    val time = String.valueOf(System.currentTimeMillis)
    val uuid = UUID.randomUUID.toString
    val nonce = uuid.replaceAll("-", "")
    val httpURI = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_URI.get().toString
    val body = ""
    val signaterGenerator = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATION_SIGNATER_GENERATOR.get().toString
    splitted = signaterGenerator.split("#")
    val args = List(appSecret, String.valueOf(myPartyId), role, appKey, time, nonce, httpURI, body).mkString(",")

    logDebug(s"${splitted(1)} of ${splitted(0)} calling")
    val signature = MethodUtils.invokeMethod(authenticator, splitted(1), args.split(",", -1):_*).asInstanceOf[String]
    logDebug(s"${splitted(1)} of ${splitted(0)} called, signature=${signature}")

    val authInfo: JSONObject = new JSONObject
    authInfo.put("signature", signature.toString)
    authInfo.put("appKey", appKey.toString)
    authInfo.put("timestamp", time.toString)
    authInfo.put("nonce", nonce.toString)
    authInfo.put("role", role.toString)
    authInfo.put("httpUri", httpURI)
    logDebug(s"authInfo to be sent=${authInfo}")
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

    val authInterface = RollSiteConfKeys.EGGROLL_ROLLSITE_POLLING_AUTHENTICATOR_INTERFACE.get().toString
    val splitted = authInterface.split("#")
    val authenticator = Class.forName(splitted(0)).newInstance()

    logDebug(s"${splitted(1)} of ${splitted(0)} calling")
    try {
      val result = MethodUtils.invokeExactMethod(authenticator, splitted(1), authUrl, heads, body).asInstanceOf[Boolean]
      logDebug(s"${splitted(1)} of ${splitted(0)} called")
      result
    } catch {
      case e: java.lang.reflect.InvocationTargetException =>
        if (e.getTargetException.isInstanceOf[java.net.ConnectException]) {
          logError(s"server authenticate failed to connect to ${authUrl}")
          throw new java.net.ConnectException(s"polling server authenticate failed to connect to authentication server")
        } else {
          throw new Exception(s"failed to authenticate, please check polling client authentication info=${authString}", e)
        }
      case t: Throwable =>
        throw new AuthenticationException(s"failed to authenticate, please check polling client authentication info=${authString}", t)
    }
  }
}
