package concurrent

/**
 * TreadLocal为每一个线程提供变量副本（底层通过Map维护），每个线程独立维护自己的副本
 * 与同步互斥不同，采用空间换时间
 * Created by bbq on 2015/11/19
 */
class ThreadLocalDemo {
  val seqNum = new ThreadLocal[Int](){
    override def initialValue(): Int = 0
  }

  def getNextNum = {
    seqNum.set(seqNum.get()+1)
    seqNum.get()
  }
}

object ThreadLocalDemo {
  def main(args: Array[String]) = {
    val num = new ThreadLocalDemo()
    (0 to 3) foreach(x=>{
      new TaskClient(num).start()
    })
  }
}
class TaskClient(sn:ThreadLocalDemo) extends Thread{
  override def run(): Unit = {
    (0 to 10)foreach(num=>{
      println("Thread"+Thread.currentThread().getName+"---"+sn.getNextNum)
    })
  }
}