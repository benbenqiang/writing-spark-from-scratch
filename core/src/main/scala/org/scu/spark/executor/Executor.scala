package org.scu.spark.executor

import java.lang.management.ManagementFactory
import java.net.URL
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap


import org.scu.spark.deploy.SparkHadoopUtil

import scala.collection.mutable

import scala.collection.JavaConverters._

import org.scu.spark.scheduler.{IndirectTaskRsult, DirectTaskResult, Task}
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

  /**task任务返回的大小，默认1GB*/
  private val maxResultSize = Utils.getMaxResultSize(conf)
  /**通过RPC直接发送给Dirver的大小，默认512MB*/
  private val maxDireactResultSize = conf.getLong("spark.task.maxDirectResultSize",1) * 1024 * 1024 *512

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

      executorBackend.statusUpdate(taskId,TaskState.RUNNING,EMPTY_BYTE_BUFFER)

      var taskStart : Long = 0
      startGCTime = computeTotalGCTime()

      try{
        /**将任务反序列化出来，包含相关依赖，以及Task*/
        val (taskFiles,taskJars,taskBytes) = Task.deserializeWithDependencies(serializedTask)
        updateDependencies(taskFiles,taskJars)
        task = ser.deserialize[Task[Any]](taskBytes,Thread.currentThread().getContextClassLoader)
        //TODO set TaskMemoryManeger

        /**task被kill，抛出异常在catch中处理异常*/
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
        }
        val taskFinish = System.currentTimeMillis()

        /**task被kill，抛出异常在catch中处理异常*/
        if(killed)
          throw new TaskKilledException
        /**对结果进行序列化*/

        val resultSer = env.serializer.newInstance()
        /**记录序列化前后的花费的时间*/
        val beforeSerilization = System.currentTimeMillis()
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()


        //TODO 记录一些序列化消耗，GC时间，任务运算消耗的时间

        //TODO task collectAccumulator

        /**对结果进行序列化，返回给driver,有两种方式，若数据量不是很大，则直接通过RPC传输给driver，若较大则保存在blockManager中*/
        val directResult = new DirectTaskResult(valueBytes)
        val serializedDireactResult = ser.serialize(directResult)
        val resultSize = serializedDireactResult.limit()

        val serializedResult : ByteBuffer = {
          /**结果大于1G的task结果将会警告*/
          if(maxResultSize > 0 && resultSize > maxResultSize){
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize. dropping it ")
            //TODO use IndireactTaskResult
            ser.serialize(new IndirectTaskRsult(resultSize))
          }else if (resultSize > maxDireactResultSize){
            //TODO add to BlockManager
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes sent via BlockManger")
            ser.serialize(new IndirectTaskRsult(resultSize))
          }else{
            logInfo(s"Finished $taskName (TI $taskId). $taskId bytes result sent to driver")
            serializedDireactResult
          }
        }

        executorBackend.statusUpdate(taskId,TaskState.FINISHED,serializedResult)
      } catch {
         //TODO catch error reason and send back to drier
        case _ : Throwable=>
      } finally {
        runningTasks.remove(taskId)
      }
    }

    /**
     * 下载目前不存在的依赖和文件，同时添加新的jar到classloader中。
     * */
    private def updateDependencies(newFile:mutable.HashMap[String,Long],newJars:mutable.HashMap[String,Long]): Unit ={
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
