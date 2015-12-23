package org.scu.spark.deploy.master

import java.util.Date

import akka.actor.ActorRef
import org.scu.spark.deploy.ApplicationDescription
import org.scu.spark.deploy.master.ApplicationState.ApplicationState

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
 * ApplicationDescription是appclient传来的，ApplicationInfo是master用于维护app用的
 * Created by bbq on 2015/12/17
 */
private[spark] class ApplicationInfo(
                                    val startTime:Long,
                                    val id:String,
                                    val desc:ApplicationDescription,
                                    val summitDate:Date,
                                    val driver:ActorRef,
                                    defaultCores:Int
                                      )extends Serializable{
  @transient var state : ApplicationState.Value = _
  @transient var executors : collection.mutable.HashMap[Int,ExecutorDesc] = _
  @transient var removedExecutors : ArrayBuffer[ExecutorDesc] = _
  @transient var coresGranted : Int = _
  @transient var endTime : Long = _
  //TODO applicationSource
  //TODO appUiUrlAtHistroyServer

  @transient private var nextExecutorId:Int = _

  private def init(): Unit ={
    state = ApplicationState.WAITING
    executors = new collection.mutable.HashMap[Int,ExecutorDesc]
    coresGranted = 0
    nextExecutorId = 0
    removedExecutors = new ArrayBuffer[ExecutorDesc]
  }

  init()

  private def newExecutorId(useID:Option[Int]=None):Int={
    useID match {
      case Some(id) =>
        nextExecutorId = math.max(nextExecutorId,id+1)
        id
      case None =>
        val id = nextExecutorId
        nextExecutorId += 1
        id
    }
  }

  private val requestedCores = desc.maxCores.getOrElse(defaultCores)

  private[master] def coresLeft :Int = requestedCores - coresGranted

  /**在applicationInfo中，添加executorDesc，并返回*/
  private[master] def addExecutor(worker:WorkerInfo,
                                   cores:Int,
                                   useId:Option[Int]=None)={
    val exec = new ExecutorDesc(newExecutorId(useId),this,worker,cores,desc.memoryPerExecutorMB)
    executors(exec.id) = exec
    coresGranted += cores
    exec
  }
}
