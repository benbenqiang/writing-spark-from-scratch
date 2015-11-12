package org.scu.spark.deploy

/**
 * Master和Woker之间传递的消息
 * Created by bbq on 2015/11/12.
 */
object DeployMessage {
  case class RegisterWorker()
  case class RegisteredWorker()
}
