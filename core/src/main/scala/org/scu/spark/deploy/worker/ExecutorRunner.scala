package org.scu.spark.deploy.worker

import java.io.File

import org.scu.spark.deploy.DeployMessage.ExecutorStateChanged

import scala.collection.JavaConverters._

import akka.actor.ActorRef
import org.scu.spark.{Logging, SparkConf}
import org.scu.spark.deploy.ApplicationDescription
import org.scu.spark.rpc.akka.AkkaRpcEnv

/**
 * 对executor进程进行管理
 * Created by bbq on 2015/12/23
 */
private[deploy] class ExecutorRunner(
                                    val appId:String,
                                    val execId:Int,
                                    val appDesc:ApplicationDescription,
                                    val cores:Int,
                                    val memory:Int,
                                    val worker:ActorRef,
                                    val workerId:String,
                                    val host:String,
                                    //TODO webUiPort,
                                    val publicAddress:String,
                                    val sparkHome:File,
                                    val executorDir:File,
                                    val workerUrl:String,
                                    conf:SparkConf,
                                    //TODO appLocalDirs:Seq[String]
                                    @volatile var state :ExecutorState.Value
                                      ) extends Logging{
  private val fullId = appId + "/" + execId
  private var workerThread :Thread = null
  private var process :Process = null

  //TODO FileAppender

  private[worker] def start(): Unit = {
    workerThread = new Thread("ExecutorRunner for " + fullId){
      override def run(): Unit = {fetchAndRunExecutor()}
    }
    workerThread.start()

    //TODO ShutDownHook
  }

  /**将变量名转换为真实值，因为Appclient在提交的时候并不知道master可以分配
    * 多少资源，所以就先用字符串代替，在ExecturorRunner中再进行替换*/
  private[worker] def substituteVariables(arguments:String):String=arguments match {
    case "{{WORKER_URL}}" => workerUrl
    case "{{EXECUTOR_ID}}" => execId.toString
    case "{{HOSTNAME}}" => host
    case "{{CORES}}" => cores.toString
    case "{{APP_ID}}" => appId
    case other => other
  }

  /**根据ApplicationDescription*/
  private def fetchAndRunExecutor(): Unit ={
    try{
      logDebug("try to run Executor with ProcessBuilder")
      /**以java -cp 的方式运行CoarseGrainedExecutorBackend*/
      val builder = CommandUtils.buildProcessBuilder(appDesc.command,memory,sparkHome.getAbsolutePath,substituteVariables)
      val command = builder.command()
      val formattedCommand = command.asScala.mkString("\"","\" \"","\"")
      logInfo(s"Launch command: $formattedCommand")

      builder.directory(executorDir)

      //TODO spark_launch_with_scala

      //TODO Loger

      process = builder.start()

      val exitCode = process.waitFor()
      state = ExecutorState.EXITED
      worker ! ExecutorStateChanged(appId,execId,state,Some("exit"),Some(exitCode))

    }catch{
      case interrupted : InterruptedException=>
        logInfo("Runner thread for executor "+fullId+"interrupted")
        state = ExecutorState.KILLED
        //TODO killProcess
      case e:Exception=>
        logError("Error running executor : \n"+e.fillInStackTrace() +"\n"+e.getStackTrace.mkString("\n"))
        state = ExecutorState.FAILED
        //TODO killProcess
    }
  }
}
