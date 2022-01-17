package com.wyh.scala.swing

import scala.swing.{Button, Frame, MainFrame, SimpleSwingApplication}

/**
 * GUI 编程示例
 * @author WangYuhang
 * @since 2022-01-14 17:57
 * */
object FirstSwingApp extends SimpleSwingApplication{
  override def top: Frame = new MainFrame{
    title = "First Swing App"
    contents = new Button{
      text = "Click me"
    }
  }
}
