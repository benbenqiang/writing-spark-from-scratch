package org.scu.spark.util

import java.util.concurrent._

import com.google.common.util.concurrent.ThreadFactoryBuilder

/**
 * Created by bbq on 2015/12/15
 */
object ThreadUtils {

  /**
   *  threadFactory:给线程添加前缀
   */
  def namedThreadFactory(prefix:String):ThreadFactory={
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix+"-%d").build()
  }

  /**
   * 固定大小的线程池.
   * 可以添加前缀，指定最大的线程数，可以设定core线程的存活时间，默认60s
   */
  def newDeamonCachedThreadPool(
                               prefix:String,
                               maxThreadNumber:Int,
                               keepAliveSeconds:Int = 60
                                 ):ThreadPoolExecutor={
    val threadFactory = namedThreadFactory(prefix)
    val threadPool = new ThreadPoolExecutor(
    maxThreadNumber,
    maxThreadNumber,
    keepAliveSeconds,
    TimeUnit.SECONDS,
    new LinkedBlockingQueue[Runnable],
    threadFactory)
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }

  /**
   *  创建固定大小的线程池,与newDeamonCachedThreadPool不同，保持core线程，不会timeout。
   *  可以添加前缀
   */
  def newDaemonFixedThreadPool(nThread:Int,prefix:String):ThreadPoolExecutor={
    val threadFactory = namedThreadFactory(prefix)
    Executors.newFixedThreadPool(nThread,threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * 周期线程
   */
  def newDaemonSingleThreadScheduledExecutor(threadName:String):ExecutorService={
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadFactory).build()
    val executor = new ScheduledThreadPoolExecutor(1,threadFactory)
    /**task一被cancell，就会从work queue中移除，否则直到delay time*/
    executor.setRemoveOnCancelPolicy(true)
    executor
  }
}
