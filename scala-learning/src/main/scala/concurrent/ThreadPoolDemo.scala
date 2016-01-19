package concurrent

import java.util.concurrent.{TimeUnit, Executors, Executor}

/**
 * 1.减少创建和销毁线程的次数，每个线程可以重复使用。
 * 2.可以控制线程的个数，以及排队策略
 *
 * 线程池运行原理：1.新线程添加首先查看corePollSize大小，未满则直接创建，若满则添加进队列
 *               2.若队列添加失败（到达容量）则直接创建线程，若当前线程大小小于maxPollsize创建成功，等于则失败
 * 配置线程池比较复杂，JavaDoc中建议使用Executors中的常用的一些静态工厂，生成线程池
 * 相关学习网站；http://www.oschina.net/question/565065_86540
 * Created by bbq on 2015/11/13
 */
object ThreadPoolDemo {

  /**
   * 创建一个单线程的线程池。
   */
  def SingleThreadDemo(threads:Array[Thread]) = {
    val pool = Executors.newSingleThreadExecutor()
    threads.foreach(pool.execute)
    pool.shutdown()
  }

  /**
   * 固定大小的线程池
   */
  def fixedThreadDemo(threads:Array[Thread]) = {
    val pool = Executors.newFixedThreadPool(2)
    threads.foreach(pool.execute)
    pool.shutdown()
  }

  /**
   * 缓存线程池，不限制大小。智能增加减少。
   */
  def cachedThreadDemo(threads:Array[Thread]) = {
    val pool = Executors.newCachedThreadPool()
    threads.foreach(pool.execute)
    pool.shutdown()
  }

  /**
   * 创建一个
   */
  def scheduledThreadDemo(threads:Array[Thread]) = {
    val pool = Executors.newScheduledThreadPool(1)
    pool.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        println("===========")
      }
    },1000,1000,TimeUnit.MILLISECONDS)
    pool.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        println(System.nanoTime())
      }
    },1000,500,TimeUnit.MILLISECONDS)
  }


  class MyThread extends Runnable {
    override def run(): Unit = {
      for (i <- 1 to 10) {
        Thread.sleep(100)
        println(Thread.currentThread().getName + " is running")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    //创建十个线程
    val threads :Array[Thread]= Array.fill[Thread](10)(new Thread(new MyThread()))
//    SingleThreadDemo(threads)
//    fixedThreadDemo(threads)
    cachedThreadDemo(threads)
//    scheduledThreadDemo(threads)

  }
}
