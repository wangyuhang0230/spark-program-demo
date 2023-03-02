package com.wyh.scala.basic

/**
 * @author WangYuhang
 * @since 2023-02-27 11:03
 * */
object CommonTypeTest {
  def main(args: Array[String]): Unit = {

    val str = "\\x0B\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00"

    val strings = str.split("\\\\x")

    println(strings.mkString(", "))

    val result = strings
      .filter(s => !"".equals(s))
      .map(s => Integer.parseInt(s, 16).toChar)

    println(result.mkString(", "))

    println(String.valueOf(result))










  }

}
