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

  /**使用进程运行自定义类*/
  def demo3()={
    val p = new ProcessBuilder("concurrent.FutureDemo")
    p.start()
  }
  def main(args: Array[String]) {
    demo3()
  }
}

class RunningProcess(name:String){
  private val thread  = new Thread("runing thread"){
    override def run(): Unit = {
      for(i <- 0 to 10){
        println(Thread.currentThread().getName + "counting " + i)
        Thread.sleep(1000)
      }
    }
  }
}
object RunningProcess{
  def main(args: Array[String]) {
    println("Starting Process : " + args(0))
    new RunningProcess("my little thread").thread.start()
  }
}

