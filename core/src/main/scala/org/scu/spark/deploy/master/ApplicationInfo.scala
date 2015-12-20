package org.scu.spark.deploy.master

import java.util.Date

import akka.actor.ActorRef
import org.scu.spark.deploy.ApplicationDescription
import org.scu.spark.deploy.master.ApplicationState.ApplicationState

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
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

  private val requestedCores = desc.maxCores.getOrElse(defaultCores)

  private[master] def coresLeft :Int = requestedCores - coresGranted
}
