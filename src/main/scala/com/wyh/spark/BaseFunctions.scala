package com.wyh.spark

import java.util.Scanner

trait BaseFunctions {

  def awaitCommand(): Unit = new Scanner(System.in).nextLine()

}
