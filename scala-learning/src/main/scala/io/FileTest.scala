package io

import java.io.File


/**
 * Created by bbq on 2015/12/23
 */
object FileTest {
  def demo1={
    val workeDir = new File(".","work")
    workeDir.mkdir()
    println(workeDir.getAbsolutePath)
  }

  def main(args: Array[String]) {
    demo1
  }
}
