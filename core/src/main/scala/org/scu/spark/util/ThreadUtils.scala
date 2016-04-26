package org.scu.spark.util

import java.util.concurrent._

import com.google.common.util.concurrent.{MoreExecutors, ThreadFactoryBuilder}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

/**
 * Created by bbq on 2015/12/15
 */
object ThreadUtils {

  /**创建一个ExecutorService，这是一个直接提交的ExecutorService，每次一submit线程其实就是调用的run方法，而不是用Thread运行
    * 具体查看Java ExecutorService API文档
    * */
  private val sameThreadExecutionContext = ExecutionContext.fromExecutorService(MoreExecutors.newDirectExecutorService())

  /**
   *  threadFactory:给线程添加前缀
   */
  def namedThreadFactory(prefix:String):ThreadFactory={
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix+"-%d").build()
  }

  /**为无限制大小的Cached线程池指定名称*/
  def newDeamonCachedThreadPool(prefix:String):ThreadPoolExecutor ={
    val threadFactory = namedThreadFactory(prefix)
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
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
  def newDaemonSingleThreadScheduledExecutor(threadName:String):ScheduledThreadPoolExecutor={
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    val executor = new ScheduledThreadPoolExecutor(1,threadFactory)
    /**task一被cancell，就会从work queue中移除，否则直到delay time*/
    executor.setRemoveOnCancelPolicy(true)
    executor
  }


  def sameThread : ExecutionContextExecutor = sameThreadExecutionContext

}
