package concurrent

/**
 * 最常用的线程之间协作，阻塞等待
 * wait释放对象琐
 * Created by bbq on 2015/11/19
 */
object WaitNotifyDemo {
  def main(args: Array[String]) {
    val lockObject = "object"
    new ThreadA(lockObject).start()
    Thread.sleep(100)
    new ThreadB(lockObject).start()
  }
}

class ThreadA(lockObject:String) extends Thread{
  override def run()= {
    while(true){
      lockObject.synchronized{
        lockObject.wait()
        println("ThreadName:"+Thread.currentThread().getName+"被唤醒")
        Thread.sleep(100)
      }
    }
  }
}


class ThreadB(lockObject:String) extends Thread{
  override def run(): Unit = {
    while(true){
      lockObject.synchronized{
        lockObject.notifyAll()
        println("ThreadName:"+Thread.currentThread().getName+"唤醒所有线程")
      }
      Thread.sleep(500)
    }
  }
}
