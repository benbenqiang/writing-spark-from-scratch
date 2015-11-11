package remote.calculator

import akka.actor.{Actor, Props}

/**
 * 如果接受到计算任务，每接受一个任务创建一个CalculatorActor
 * 如果接受到的是计算结果，则输出，并停止子actor
 * Created by bbq on 2015/11/11
 */
class CreationActor extends Actor{
  override def receive: Receive = {
    case op:MathOp =>
      context.actorOf(Props[CalculatorActor]) ! op
    case MultiplicationResult(n1,n2,r) =>
      println(s"Mul result $n1 * $n2 = $r")
      context.stop(sender())
    case DivisionResult(n1,n2,r) =>
      println(s"Div result $n1 / $n2 = $r")
      context.stop(sender())
  }
}
