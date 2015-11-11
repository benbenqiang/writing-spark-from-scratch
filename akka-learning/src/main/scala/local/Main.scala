package local

import akka.actor._
import akka.actor.Actor.Receive

/**
 * actor本地模式demo
 * Created by applelab on 2015/11/10
 */

//actor之间通讯都是使用case class
case class Done()
case class Greet()


object Main {
  def main(args: Array[String]) {
    //启动actor系统
    val system = ActorSystem("greeting_system")
    //初始化receive actor
    val receive_actor = system.actorOf(Props[Receiver],"receive")
    //初始化sender actor 以 receive actor 的ref 为参数进行初始化
    val sender_actor = system.actorOf(Props(classOf[Sender],receive_actor),"sender")
    //初始化监听actor，监督receive的状态，若actor停止，则关闭系统
    system.actorOf(Props(classOf[Watcher],receive_actor))
  }
}

/**
 * 监听Actor，受监督actor停止后，关闭system
 * @param ref : actorRef
 */
class Watcher(ref:ActorRef) extends Actor with ActorLogging{
  context watch ref

  override def receive: Actor.Receive = {
    case Terminated(_) =>
      log.info("{} has terminated ,shutting down system",ref.path)
      context.system.shutdown()
  }
}

/**
 * 接受信息的Actor
 */
class Receiver extends Actor{
  override def receive: Receive = {
    case Greet =>
      println("receiving greeting")
      sender ! Done
    case Done =>
      println("receiver shutdown")
      context.stop(self)
  }
}

/**
 * 发送信息的actor
 * @param actor：actorRef
 */
class Sender(actor:ActorRef) extends Actor{

  override def preStart()={
    actor ! Greet
  }

  override def receive: Actor.Receive = {
    case Done =>
      println("back message")
      sender ! Done
      context.stop(self)
  }
}