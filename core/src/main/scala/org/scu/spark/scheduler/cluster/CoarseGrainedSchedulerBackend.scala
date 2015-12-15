package org.scu.spark.scheduler.cluster

import akka.actor.Actor.Receive
import akka.actor.{Actor, Props, ActorRef}
import org.scu.spark.Logging
import org.scu.spark.rpc.akka.AkkaRpcEnv
import org.scu.spark.scheduler.{SchedulerBackend, TaskSchedulerImpl}

import scala.collection.mutable.ArrayBuffer

/**
 * 一个粗粒度的调度器，为一个job分配固定的资源，为多个task共享，
 * 而不是运行完一个归还资源，并且为新Task重新分配资源。
 * Created by bbq on 2015/12/11
 */
private[spark] class CoarseGrainedSchedulerBackend(scheduler:TaskSchedulerImpl,val rpcEnv:AkkaRpcEnv)
extends SchedulerBackend{

  val conf = scheduler.sc.conf

  /** dirver程序的RPC对象*/
  var driverEndPoint : ActorRef = _


  class DriverEndPoint(val rpcEnv:AkkaRpcEnv,sparkProperties:Seq[(String,String)]) extends Actor with Logging{
    override def receive: Receive = ???
  }

  override def start(): Unit = {
    val properties = new ArrayBuffer[(String,String)]
    /**从conf中读取properties*/
    for((key,value) <- scheduler.sc.conf.getAll){
      if(key.startsWith("spark.")){
        properties += ((key,value))
      }
    }

    driverEndPoint = rpcEnv.doCreateActor(Props(classOf[String]),CoarseGrainedSchedulerBackend.ENDPOINT_NAME)

  }

  override def stop(): Unit = ???

  override def defaultParallelism(): Int = ???

  override def reviveOffers(): Unit = ???
}

private[spark] object CoarseGrainedSchedulerBackend{
  val ENDPOINT_NAME = "CoraseGrainedScheduler"
}
