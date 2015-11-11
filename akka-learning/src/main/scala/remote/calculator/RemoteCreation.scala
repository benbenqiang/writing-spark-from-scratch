package remote.calculator


import scala.concurrent.duration._
import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory

import scala.util.Random

/**
 * 连接RemoteWorker，并通过远程连接在remoteWorker中初始化actor完成任务计算。
 * 也就是在remoteWorker中执行任务
 * Created by applelab on 2015/11/11
 */
object RemoteCreation extends App{
  val system = ActorSystem("CreationSystem",ConfigFactory.load("remotecreation"))
  val actor = system.actorOf(Props[CreationActor],"creationActor")

  println("Started CreationSystem")
  import system.dispatcher
  system.scheduler.schedule(1.second,1.second){
    if(Random.nextInt(100) % 2 ==0)
      actor ! Multiply(Random.nextInt(20),Random.nextInt(20))
    else
      actor ! Divide(Random.nextInt(10000), Random.nextInt(99) + 1)
  }
}
