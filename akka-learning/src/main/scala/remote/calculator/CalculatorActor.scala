package remote.calculator

import akka.actor.Actor

/**
 * 接受计算任务，并返回Result
 * Created by bbq on 2015/11/11
 */
class CalculatorActor extends Actor{
  override def receive: Receive = {
    case Add(n1,n2) =>
      println(s"calculating $n1 + $n2")
      sender() ! AddResult(n1,n2,n1+n2)
    case Subtract(n1,n2) =>
      println(s"calculating $n1 - $n2")
      sender() ! SubtractResult(n1,n2,n1-n2)
    case Multiply(n1,n2)=>
      println(s"calculating $n1 * $n2")
      sender() ! MultiplicationResult(n1,n2,n1*n2)
    case Divide(n1,n2)=>
      println(s"calculating $n1 / $n2")
      sender() ! DivisionResult(n1,n2,n1/n2)
  }
}
