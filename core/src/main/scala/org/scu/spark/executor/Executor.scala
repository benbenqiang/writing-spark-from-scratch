package org.scu.spark.executor

import java.io.File
import java.lang.management.ManagementFactory
import java.net.URL
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap


import org.scu.spark.deploy.SparkHadoopUtil

import scala.collection.mutable
import scala.collection.mutable._
import scala.collection.JavaConverters._

import org.scu.spark.scheduler.Task
import org.scu.spark.util.{Utils, ThreadUtils}
import org.scu.spark.{TaskKilledException, TaskState, Logging, SparkEnv}

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

  private val currentFIles = new mutable.HashMap[String,Long]()
  private val currentJars = new mutable.HashMap[String,Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private val conf = env.conf
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

    @volatile private var killed = false

    /**任务运行之前，JVM的GC time*/
    @volatile var startGCTime : Long = _

    @volatile var task:Task[Any] = _

    /**JVM GC时间*/
    private def computeTotalGCTime():Long = {
      ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
    }


    override def run(): Unit = {
      //TODO TaskMemoryManager
      val deserializeStartTime = System.currentTimeMillis()
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")
      executorBackend.startsUpdate(taskId,TaskState.RUNNING,EMPTY_BYTE_BUFFER)
      var taskStart : Long = 0
      startGCTime = computeTotalGCTime()

      try{
        /**将任务反序列化出来，包含相关依赖，以及Task*/
        val (taskFiles,taskJars,taskBytes) = Task.deserializeWithDependencies(serializedTask)
        updateDependencies(taskFiles,taskJars)
        task = ser.deserialize[Task[Any]](taskBytes,Thread.currentThread().getContextClassLoader)
        //set TaskMemoryManeger
        if(killed)
          throw new TaskKilledException

        //TODO MapOutputTracker

        taskStart = System.currentTimeMillis()
        var threwException = true
        val value = try{
          /**task开始运行*/
          val res = task.run(taskId,attemptedNumber)
          threwException = false
          res
        }finally {
          //TODO 释放相关资源和内存

          val taskFinish = System.currentTimeMillis()

          /**对结果进行序列化*/

        }


      }
    }

    /**
     * 下载目前不存在的依赖和文件，同时添加新的jar到classloader中。
     * */
    private def updateDependencies(newFile:HashMap[String,Long],newJars:HashMap[String,Long]): Unit ={
      lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
      synchronized{
        /**有更新的jar包或者文件版本才进行更新*/
        for((name,timestamp) <- newFile if currentFIles.getOrElse(name,-1L) < timestamp){
          logInfo("Fetching" + name + " with timestamp" + timestamp)
          /**将文件或者目录拷贝到sparkLocalDir,同一个Executor的多个task就可以共享这个文件或目录*/
          // TODO Utils.fetchFile
        }
        for((name,timestamp) <- newJars){
          //TODO
          ???
        }
      }
    }
  }

}
