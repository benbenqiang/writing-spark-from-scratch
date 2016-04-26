package org.scu.spark.executor

import java.net.URL
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import org.scu.spark.util.ThreadUtils
import org.scu.spark.{Logging, SparkEnv}

/**
 * Created by bbq on 2016/1/13
 */
class Executor(
              executorId:String,
              executorHostname:String,
              env:SparkEnv,
              userClassPath:Seq[URL]=Nil,
              isLocal:Boolean=false
                ) extends Logging{

  logInfo(s"Starting executor ID $executorId on host $executorHostname")

  /**当前正在运行的任务*/
  private val runningTasks = new ConcurrentHashMap[Long,TaskRunner]()

  /**用于任务提交的线程池，有无限容量*/
  private val threadPool = ThreadUtils.newDeamonCachedThreadPool("Executor task launch worker")

  def launchTask(
                context:ExecutorBackend,
                taskId:Long,
                attemptNumber:Int,
                taskName:String,
                serializedTask:ByteBuffer
                  ):Unit={
    val tr = new TaskRunner(context,taskId,attemptNumber,taskName,serializedTask)
    runningTasks.put(taskId,tr)
    threadPool.execute(tr)
  }

  /**运行task的主体*/
  class TaskRunner(
                  executorBackend: ExecutorBackend,
                  val taskId:Long,
                  val attemptedNumber:Int,
                  taskName:String,
                  serializedTask:ByteBuffer
                    )extends Runnable{
    override def run(): Unit = {

    }
  }

}
