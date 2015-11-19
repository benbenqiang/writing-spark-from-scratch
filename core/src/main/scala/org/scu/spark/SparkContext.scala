package org.scu.spark

import java.util.concurrent.atomic.AtomicInteger

import org.scu.spark.rdd.{ParallelCollectionRDD, RDD}
import org.scu.spark.scheduler.DAGScheduler

/**
 * 1.spark的主要入口。spark用于连接集群，获取Executor资源。
 * 2.Spark运算流程：
 * sparkContext创建RDD，触发action，通过DAGScheduler形成DAG，转化成TaskSet，
 * TaskSchedulerImpl通过SparkDeploySchedulerBackend的reviveOffers，向ExecutorBackend发送LaunchTask消息，开始在集群中计算
 * Created by bbq on 2015/11/10
 */
class SparkContext extends Logging {

  @volatile private var _dagScheduler: DAGScheduler = _

  /**
   * 下一个RDD的ID
   */
  val nextRddId = new AtomicInteger(0)
  def newRddId() = nextRddId.getAndIncrement()

  private def dagScheduler :DAGScheduler = _dagScheduler
  private def dagScheduler_= (ds:DAGScheduler):Unit = {
    _dagScheduler = ds
  }

  def parallelize[T](seq: Seq[T], numSlices: Int = 3) = {
    new ParallelCollectionRDD[T](this, seq, numSlices)
  }

  /**
   * 对所有的partition进行计算
   */
  def runJob[T, U](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, rdd.partitions.indices)
  }

  def runJob[T, U](rdd: RDD[T], func: Iterator[T] => U, partitions: Seq[Int]): Array[U] = {
    runJob[T,U](rdd, (ctx: TaskContext, it: Iterator[T]) => func(it), partitions)
  }

  /**
   * 计算部分Partition，并将每个Partition的结果存入Array
   */
  def runJob[T, U](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U, partitions: Seq[Int]): Array[U] = {
    val results = new Array[U](partitions.size)
    runJob[T,U](rdd, func, partitions, (index, res) => results(index) = res)
    results
  }

  /**
   * 这里是所有action的入口，通过resultHandler返回结果
   */
  def runJob[T, U](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U, partitions: Seq[Int], resultHandler: (Int, U) => Unit): Unit = {
    dagScheduler.runJob(rdd,func,partitions,resultHandler)
  }
}

object SparkContext {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    val array: Array[Int] = Array(1, 2, 3, 4, 6, 7, 8, 9, 10)
    val a: ParallelCollectionRDD[Int] = sc.parallelize(array, 10)
    a.map(_ * 2)
  }
}