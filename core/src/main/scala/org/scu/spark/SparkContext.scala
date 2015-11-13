package org.scu.spark

import akka.actor.{Actor, Props}
import org.scu.spark.rpc.akka.{AkkaRpcEnv, AkkaUtil, RpcEnvConfig}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
 * 1.spark的主要入口。spark用于连接集群，获取Executor资源。
 * 2.Spark运算流程：
 * sparkContext创建RDD，触发action，通过DAGScheduler形成DAG，转化成TaskSet，
 * TaskSchedulerImpl通过SparkDeploySchedulerBackend的reviveOffers，向ExecutorBackend发送LaunchTask消息，开始在集群中计算
 * Created by bbq on 2015/11/10
 */
class SparkContext extends Logging {

}

object SparkContext {
  def main(args: Array[String]) {
    //RegisteredWorker.asInstanceOf[RegisterWorkerResponse]
    import scala.concurrent.ExecutionContext.Implicits.global
    val future = Future {
      Thread.sleep(4000)
      1+1
    }
//    val result = Await.result(future,2 seconds)
//    println(result)
    future.onComplete{
      case Success(msg) => println("onComplete:"+msg)
      case Failure(msg) => println("Faild"+msg)
    }
  }
}