package remote.benchmark

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
 * 远程Actor实例：
 * 接受Sender发送来的批量信息，并反馈
 * akka中消息传递使用的都是case class 解释请见:scala-learning:CaseClassDemo.scala
 * Created by bbq on 2015/11/11.
 */

case object ShutDonw   //全局共享 单例唯一的对象
sealed trait Echo      //继承sealed的类在模式匹配中必须全部出现
case object Start extends Echo
case object Done extends Echo
case class Continue(remaining:Int,startTime:Long,burstStartTime:Long,n:Int) extends Echo

object Receiver extends App{
  //初始化system，从配置文件加载配置信息
  val system = ActorSystem("ReceiverSys",ConfigFactory.load("remotelookup"))
  system.actorOf(Props[Receiver],"rcv")
}

class Receiver extends Actor{
  override def receive: Receive = {
    case m:Echo => sender() ! m
    case ShutDonw => context.system.shutdown()
    case _ =>
  }
}