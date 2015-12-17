package org.scu.spark.deploy

import akka.actor.ActorRef

/**
 * Master和Woker之间传递的消息
 * Created by bbq on 2015/11/12.
 */

sealed trait DeployMessage

object DeployMessage {
  /**
   * Work向Master注册时所需要用到的消息
   */
  case class RegisterWorker(id:String,host:String,port:Int,cores:Int,memory:Int)

  sealed trait RegisterWorkerResponse
  case class RegisteredWorker() extends RegisterWorkerResponse with DeployMessage
  case class RegisterWorkerFaild(message:String) extends RegisterWorkerResponse with DeployMessage

  case class Heartbeat(workerId: String) extends DeployMessage
  case object SendHeartbeat

  /**
   *  AppClient to Master
   */
  case class RegisterApplication(description: ApplicationDescription) extends DeployMessage

  /**
   * Master to Appclient
   */
  case class RegisteredApplication(appId:String,master:ActorRef)extends DeployMessage

}
