package org.scu.spark

import java.util.concurrent.atomic.AtomicInteger

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

  /**
   * 下一个RDD的ID
   */
  val nextRddId = new AtomicInteger(0)

  def newRddId() = nextRddId.getAndIncrement()

  def parallelize[T](seq:Seq[T],numSlices:Int = 3)={

  }
}

object SparkContext {
  def main(args: Array[String]) {

  }
}