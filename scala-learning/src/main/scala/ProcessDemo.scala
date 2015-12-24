/**
 * Process 调用操作系统的指令
 * 两种基本运行方式
 * Created by bbq on 2015/12/24
 */
object ProcessDemo {
  /**使用processBuilder*/
  def demo1(): Unit ={
    val p = new ProcessBuilder("notepad").start()
  }

  /**使用Runtime 的exec*/
  def demo2()={
    val p = Runtime.getRuntime.exec(Array("notepad"))
  }

  def main(args: Array[String]) {
    demo2()
  }
}
