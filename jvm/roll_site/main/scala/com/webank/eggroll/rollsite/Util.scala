package com.webank.eggroll.rollsite

import java.security.MessageDigest
import com.google.protobuf.ByteString


object Util {
  def hashMD5(content: Array[Byte]): String = {
    val md5 = MessageDigest.getInstance("MD5")
    val encoded = md5.digest(content)
    encoded.map("%02x".format(_)).mkString
  }

  def hashMD5(content: ByteString): String = {
    hashMD5(content.toByteArray)
  }

  def hashMD5(content: String): String = {
    hashMD5(content.getBytes)
  }

  def main(args: Array[String]) {
    println(hashMD5("abcdefg"))
    println(hashMD5("abcde"))
  }

}