package remote.benchmark

import akka.actor._
import com.typesafe.config.ConfigFactory
import remote.benchmark.Sender.Warmup
import scala.concurrent.duration._

/**
 * 远程Actor实例：
 * 1.创建一个Sender 和一个Receiver
 * 2.Sender向Receiver发送批量数据
 * 3.记录吞吐量并输出
 * Created by bbq on 2015/11/11
 */
object Sender {
  def main(args: Array[String]) {
    val system = ActorSystem("SenderSys",ConfigFactory.load("calculator"))
    //actor远程ip和端口
    val remoteHostPost = "127.0.0.1:2553"

    //actor的路径设计采用了类似URL的形式，即scheme://domain:port/path
    //根据systemName，remoteHostPost和username定位一个远程actor
    val remotePath = s"akka.tcp://ReceiverSys@$remoteHostPost/user/rcv"
    //一共发送多少条消息
    val totalMessage = 50000
    //每次发送多少条信息
    val burstSize = 500
    //每条信息的大小
    val payloadSize = 100
    //启动actor
    system.actorOf(Props(classOf[Sender],remotePath,totalMessage,burstSize,payloadSize))
  }

  private case object Warmup
}

class  Sender(path:String,totalMessage:Int,burstSize:Int,payloadSize:Int) extends Actor{

  //每条信息的内容
  val payload:Array[Byte] = Vector.fill(payloadSize)("a").mkString.getBytes
  var startTime = 0L
  //发送周期花的最大时间
  var maxRoundTripMills = 0L

  //隐式转换 导入了scala.concurrent.duration._ 可以很方便的将Int转化为Seconds
  context.setReceiveTimeout(3.seconds)
  sendIdentifyReques()

  //向远程actor发送连接请求
  def sendIdentifyReques()={
    context.actorSelection(path) ! Identify(path)
  }

  override def receive: Receive = {
    //连接成功
    case ActorIdentity(`path`,Some(actor)) =>
      //监听远程actor，若停止则触发Terminated消息
      context.watch(actor)
      //替换当前Actor的行为
      context.become(active(actor))
      //一定要关闭轮训时间，否则就算成功连接了也会一直尝试
      context.setReceiveTimeout(Duration.Undefined)
      self ! Warmup
    //远程服务还未启动
    case ActorIdentity(`path`,None) => println(s"Remote actor not available : $path 提示：请启动Receiver")
    //超时重发请求
    case ReceiveTimeout => sendIdentifyReques()
  }

  //连接成功后，替换新的响应行为
  def active(actor:ActorRef):Receive={
    case Warmup =>
      sendBatch(actor,totalMessage)
      actor ! Start

    case Start =>
      println(s"starting benchmark of $totalMessage message with burst size $burstSize and payload size $payloadSize")
      startTime = System.nanoTime()
      val remaining = sendBatch(actor,totalMessage)
      if (remaining == 0)
        actor ! Done
      else
        actor ! Continue(remaining,startTime,startTime,burstSize)

    case c : Continue =>
      val now =System.nanoTime()
      val duration = (now - c.startTime).nanos.toMillis
      val roudTripMillis = (now - c.burstStartTime).nanos.toMillis
      //更新最大发送周期时间
      maxRoundTripMills = math.max(maxRoundTripMills, roudTripMillis)
      if(duration > 500){
        val  throughout = (c.n*1000 /duration).toInt
        println(s"it took $duration seconds to $totalMessage message,throughtout $throughout msg/s,roundTime: $roudTripMillis ")
      }
      val remaining = sendBatch(actor,c.remaining)
      if(remaining==0)
        actor ! Done
      else if (duration > 500)
        actor ! Continue(remaining,now,now,burstSize)
      else
        actor ! Continue(remaining,c.startTime,now, burstSize + c.n) //指数增加发送带宽

    //所有传输已经完成，向远程actor发送shutdown信息
    case Done =>
      val took = (System.nanoTime() - startTime).nanos.toMillis
      val throughout = (totalMessage*1000/took).toInt
      println(s"it took $took seconds to $totalMessage message,throughtout $throughout msg/s,maxroundTime: $maxRoundTripMills ")
      actor ! ShutDonw

    //监听到远程对象关闭了
    case Terminated(`actor`) =>
      println("Receiver terminated")
      context.system.shutdown()
  }

  /**
   * @return 返回剩余的信息条数
   */
  def sendBatch(actor:ActorRef,remaining:Int):Int= {
    val batchSize = math.min(remaining,burstSize)
    (1 to batchSize) foreach(x => actor ! payload)
    remaining - batchSize
  }
}
