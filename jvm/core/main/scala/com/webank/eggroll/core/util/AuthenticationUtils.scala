package com.webank.eggroll.core.util

import java.util.Base64

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

object AuthenticationUtils extends Logging {
  private val UTF8 = "UTF-8"
  private val HMACSHA1 = "HmacSHA1"

  @throws[Exception]
  private def HmacSHA1Encrypt(encryptText: String, encryptKey: String) = {
    val data = encryptKey.getBytes(UTF8)
    val secretKey = new SecretKeySpec(data, HMACSHA1)
    val mac = Mac.getInstance(HMACSHA1)
    mac.init(secretKey)
    val text = encryptText.getBytes(UTF8)
    mac.doFinal(text)
  }

  def generateSignature(appSecret: String, encryptParams: String*): String = {
    try {
      var encryptText = ""
      for (i <- 0 until encryptParams.length) {
        if (i != 0) encryptText += "\n"
        encryptText += encryptParams(i)
      }
      encryptText = new String(encryptText.getBytes, AuthenticationUtils.UTF8)
      return Base64.getEncoder.encodeToString(AuthenticationUtils.HmacSHA1Encrypt(encryptText, appSecret))
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    null
  }
}
