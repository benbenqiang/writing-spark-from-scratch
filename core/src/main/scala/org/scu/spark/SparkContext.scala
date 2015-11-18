package org.scu.spark

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, Props}
import org.scu.spark.rdd.{RDD, ParallelCollectionRDD}
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
      new ParallelCollectionRDD[T](this,seq,numSlices)
  }

//  /**
//   *  对所有的partition进行计算
//   */
//  def runJob[T,U](rdd:RDD[T],func:Iterator[T] => U):Array[U]={
//    runJob(rdd,func,0 until rdd.partitions)
//  }
//
//  def runJob[T,U](rdd:RDD[T],func:Iterator[T]=>U,partitions:Seq[Int]):Array[U]={
//
//  }
}

object SparkContext {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    val array:Array[Int] = Array(1,2,3,4,6,7,8,9,10)
    val a:ParallelCollectionRDD[Int] = sc.parallelize(array,10)
    a.map(_*2)
  }
}