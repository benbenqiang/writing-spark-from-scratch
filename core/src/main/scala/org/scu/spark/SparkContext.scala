package org.scu.spark

import org.scu.spark.rpc.akka.RpcEnvConfig


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
    val rpcConfig = new RpcEnvConfig("master", "127.0.0.1", 2000)
//    new AkkaRpcEnvFactory().create(rpcConfig)
    println("")
  }
}