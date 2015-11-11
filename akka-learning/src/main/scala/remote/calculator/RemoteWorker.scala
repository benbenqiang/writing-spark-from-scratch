package remote.calculator

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

/**
 * 建立好actorSystem后，等待创建任务到来
 * Created by bbq on 2015/11/11.
 */
object RemoteWorker {
  def main(args: Array[String]) {
    ActorSystem("CalculatorWorkerSystem",ConfigFactory.load("calculator"))
    println("Started CalculatorWorkerSystem")
  }
}
