package org.scu.spark.scheduler.cluster

import akka.actor.ActorRef
import org.scu.spark.rpc.akka.RpcAddress

/**
 * Created by bbq on 2016/3/31
 */
private[cluster] class ExecutorData(
                                   val executorEndpoint:ActorRef,
                                   val executorAddress:RpcAddress,
                                   override val executorHost:String,
                                   var freeCores:Int,
                                   override val totalCores:Int,
                                   override val logUrlMap:Map[String,String]
                                     ) extends ExecutorInfo(executorHost,totalCores,logUrlMap)
