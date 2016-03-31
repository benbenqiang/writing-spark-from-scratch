import java.io.{FileOutputStream, File}

import scala.collection.JavaConverters._
/**
 * Process 调用操作系统的指令
 * 两种基本运行方式
 * Created by bbq on 2015/12/24
 */
object ProcessDemo {
  /**使用processBuilder*/
  def demo1(): Unit ={
    val p = new ProcessBuilder("java -cp F:\\Beijing\\technology\\spark\\.\\conf;F:\\Beijing\\technology\\spark\\.\\out\\artifacts\\core_jar\\core.jar -Xms1024M -Xmx1024M -Dspark.master.port=60000 -Dspark.driver.host=127.0.0.1 -Dspark.executor.port=7000 -Dspark.app.name=defaultAppName -Dspark.driver.port=60010 -Dspark.master.host=127.0.0.1 -Dspark.executor.memory=1024 -Dspark.executor.cores=2 -XX:MaxPermSize=256m org.scu.spark.executor.CoarseGrainedExecutorBackend").start()
  }

  /**使用Runtime 的exec*/
  def demo2()={
    val p = Runtime.getRuntime.exec(Array("notepad"))
  }

  /**使用进程运行自定义类*/
  def demo3()={
    val p = new ProcessBuilder("java","--version")
    val formatted = p.command().asScala.mkString("\"","\" \"","\"")
    println(formatted)
    p.start()
  }
  /**将结果process结果输出到文件*/
  def demo4()={
    val file = new File(System.getProperty("java.io.tmpdir"),"scala_learning")
    val p = new ProcessBuilder("java" ,"-cp", "F:\\Beijing\\technology\\spark\\.\\conf;F:\\Beijing\\technology\\spark\\.\\out\\artifacts\\core_jar\\core.jar","org.scu.spark.executor.CoarseGrainedExecutorBackend")
    val process  = p.start()
    val inputStream = process.getErrorStream
    val buf = new Array[Byte](100)
    val n = inputStream.read(buf)
    println(s"get data $n from stream : ${buf.mkString(",")}")
    val ops = new FileOutputStream(file,false)
    ops.write(buf,0,n)
  }
  def main(args: Array[String]) {
    demo4()
    Thread.sleep(10000)
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

