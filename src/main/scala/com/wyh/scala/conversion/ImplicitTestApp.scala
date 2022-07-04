package com.wyh.scala.conversion

import java.awt.event.{ActionEvent, ActionListener}
import javax.swing.JButton
import scala.language.implicitConversions

/**
 * 测试隐式转换
 *
 * @author WangYuhang
 * @since 2022-01-17 14:47
 * */
object ImplicitTestApp {

  implicit def function2ActionListener(f: ActionEvent => Unit): ActionListener = new ActionListener {
    override def actionPerformed(e: ActionEvent): Unit = f(e)
  }

  def main(args: Array[String]): Unit = {

    val button = new JButton
//    button.addActionListener(
//      new ActionListener {
//        override def actionPerformed(e: ActionEvent): Unit = {
//          println("pressed!")
//        }
//      }
//    )

    button.addActionListener(
      (_: ActionEvent) => println("pressed!")
    )

  }
  implicit class RectangleMaker(width:  Int){
    def  x(height:  Int): Rectangle =  Rectangle(width,  height)
  }
}
case class Rectangle(width: Int, height: Int)

