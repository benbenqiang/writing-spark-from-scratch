package org.scu.spark.deploy.worker

import java.io.File

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
  }

  private def fetchAndRunExecutor(): Unit ={
    try{
//      val builder =
    }catch{
      case interrupted : InterruptedException=>{
        logInfo("Runner thread for executor "+fullId+"interrupted")
        state = ExecutorState.KILLED
        //TODO killProcess
      }
      case e:Exception=>{
        logError("Error running executor"+e)
        state = ExecutorState.FAILED
        //TODO killProcess
      }
    }
  }
}
