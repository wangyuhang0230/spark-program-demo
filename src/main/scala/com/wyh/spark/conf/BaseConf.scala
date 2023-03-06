package com.wyh.spark.conf

import java.util.Properties

/**
 * @author WangYuhang
 * @since 2023-03-06 10:49
 * */
object BaseConf {

  val prop: Properties = {
    val prop = new Properties()
    prop.load(this.getClass.getResourceAsStream("/demo.properties"))
    prop
  }

  val select_month: Int = prop.getProperty("hive.select.month").toInt

  val hive_table: String = prop.getProperty("hive.table")
  val hive_saveMode: String = prop.getProperty("hive.save.mode")

  val phoenix_zkUrl: String = prop.getProperty("phoenix.zkUrl")
  val phoenix_table: String = prop.getProperty("phoenix.table")

  val doris_url: String = prop.getProperty("doris.url")
  val doris_username: String = prop.getProperty("doris.username")
  val doris_password: String = prop.getProperty("doris.password")
  val doris_table: String = prop.getProperty("doris.table")
  val doris_fenodes: String = prop.getProperty("doris.fenodes")
  val doris_dbTable: String = prop.getProperty("doris.db.table")
  val doris_saveMode: String = prop.getProperty("doris.save.mode")

  val ob_url: String = prop.getProperty("ob.url")
  val ob_table: String = prop.getProperty("ob.table")
  val ob_packet: String = prop.getProperty("ob.packet")
  val ob_saveMode: String = prop.getProperty("ob.save.mode")
  val ob_username: String = prop.getProperty("ob.username")
  val ob_password: String = prop.getProperty("ob.password")

}
