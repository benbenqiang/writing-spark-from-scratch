package org.scu.spark.scheduler

import java.util.concurrent.atomic.AtomicInteger

import org.scu.spark.rdd.RDD
import org.scu.spark.{Logging, SparkContext, TaskContext}

/**
 * 1.面向DAG的调度，将任务划分成多个Stage。
 * 2.以shuffle最为划分边界。对窄依赖进行pipeline处理。
 * 3.DAGScheduler处理shuffle文件丢失错误，会让上一个Stage重新运行，其他错误交给TaskScheduler
 * 4.每一个stage由一组相同的task组成。每个task都是独立相同的。可以并行执行，执行在不同的Partition上
 * 5.有两种stage：ResultStage，和ShuffleMapStage
 * 6.缓存跟踪：用于记录哪些RDD和shuffle文件被缓存过
 * 7.PreferedLocation：根据RDD的PF和缓存跟踪决定哪些task运行在那台机器上。
 * 8.清理没用的数据，防止long-running 内存泄露
 * Created by bbq on 2015/11/19
 */
private[spark] class DAGScheduler(
                  private val sc:SparkContext
                    ) extends Logging{

  private val nextJobId = new AtomicInteger(0)

  /**
   * 提交任务，并返回JobWaiter
   */
  def submitJob[T,U](
                    rdd:RDD[T],
                    func:(TaskContext,Iterator[T])=>U,
                    partitions:Seq[Int],
                    resultHandler:(Int,U)=>Unit
                      ): JobWaiter[U] ={
    val jobId = nextJobId.getAndIncrement()
    if(partitions.isEmpty) {
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    val waiter = new JobWaiter[U](this,jobId,partitions.size,resultHandler)


    waiter
  }
  /**
   * 计算job，并将结果传给resultHandler
   */
  def runJob[T,U](
                   rdd:RDD[T],
                   func:(TaskContext,Iterator[T])=>U,
                   partitions:Seq[Int],
                   resultHandler:(Int,U)=>Unit):Unit={
    val start = System.nanoTime()
    val waiter = submitJob(rdd,func,partitions,resultHandler)
    waiter.awaitResult()match {
      case JobSucceeded=>
      case JobFailed(e)=>
    }
  }
}
