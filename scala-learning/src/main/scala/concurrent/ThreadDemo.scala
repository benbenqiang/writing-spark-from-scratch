package concurrent

/**
 * 线程相关：
 * 1.sleep：时间不是准确的，受调度和系统计时器影响，因为线程就绪之后也不会马上被执行，sleep后会让出cpu时间，sleep时不丢失监视器所有权（锁）
 * 线程sleep的时候会从执行状态退出，到阻塞状态，当时间到了马上就绪
 * 2.join:  等待线程终止,
 * 3.线程的中断方式：interrupt 只是在线程上做了一个标记，在线程阻塞（（wait jin sleep）的时候会抛出异常,同时清楚终端状态。
 * Thread.interrupted 也会清楚状态
 * 线程想要中断，必须要自己决定。
 * 4.线程的interrupt方法太麻烦，我们用自己的方式去中断。
 * 5.优先级：当java进程获得时间片的时候，进程中的线程通过优先级来确定获取执行的概率
 *
 * Created by bbq on 2016/4/8
 */
object ThreadDemo {
  def sleepDemo = {
    val my = new SleepDemoThread
    val t1 = new Thread(my)
    val t2 = new Thread(my)
    t1.start()
    t2.start()
  }

  def joinDemo = {
    val my = new JoinDemoThread
    val t1 = new Thread(my)
    t1.start()
    (1 to 10) foreach (i => {
      println(Thread.currentThread().getName + ":" + i)

      /** 等待t1执行完成才继续执行 */
      if (i == 5) t1.join()
      Thread.sleep(500)
    })
  }

  def interuptDemo = {
    val my = new InteruptDemoThread
    val t1 = new Thread(my)
    t1.start()

    (1 to 10) foreach (i => {
      println(Thread.currentThread().getName + ":" + i)

      /** 等待t1执行完成才继续执行 */
      if (i == 5) t1.interrupt()
      Thread.sleep(500)
    })
  }
  
  def interuptByLableDemo ={
    val my = new InteruptByLableDemo
    val t1 = new Thread(my)
    t1.start()
    (1 to 10) foreach (i => {
      println(Thread.currentThread().getName + ":" + i)
      /** 等待t1执行完成才继续执行 */
      if (i == 5) my.stopThread
      Thread.sleep(500)
    })
  }

  def main(args: Array[String]) {
    //    sleepDemo
    //    joinDemo
//    interuptDemo
    interuptByLableDemo
  }
}


class SleepDemoThread extends Runnable {
  var count = 0

  override def run(): Unit = {
    (1 to 20) foreach { i => {
      /** sleep时刻不释放锁 */
      synchronized {
        println(Thread.currentThread().getName + ":" + i)
        count += 1
        println(count)
        Thread.sleep(1000)
      }
    }
    }
  }
}

class JoinDemoThread extends Runnable {
  override def run(): Unit = {
    (1 to 10) foreach (i => {
      println(Thread.currentThread().getName + ":" + i)
      Thread.sleep(500)
    })
  }
}

class InteruptDemoThread extends Runnable {
  override def run(): Unit = {
    var i = 0
    while (!Thread.interrupted() && i < 10) {
      println(Thread.currentThread().getName + ":" + i)
      try {
        Thread.sleep(500)
      } catch {
        case e: InterruptedException =>

          /** 虽然被主线程中断，但仅仅是一个标记，还会继续运行 */
          println("我被中断了")
          Thread.currentThread().interrupt()

        /** 不能用interrupted 因为会对interrupte标志进行重置 */
        //          Thread.interrupted()
      }
      i += 1
    }
  }
}

/** 自定义标记完成中断线程 */
class InteruptByLableDemo extends Runnable {
  private var flag = true

  def stopThread = {
    flag = false
  }

  override def run(): Unit = {
    var i = 0
    while (flag && i < 10) {
      println(Thread.currentThread().getName + ":" + i)
      Thread.sleep(500)
      i += 1
    }
  }
}