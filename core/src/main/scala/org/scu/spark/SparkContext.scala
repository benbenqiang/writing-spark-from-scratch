package org.scu.spark

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.lang3.SerializationUtils
import org.scu.spark.rdd.{ParallelCollectionRDD, RDD}
import org.scu.spark.rpc.akka.RpcAddress
import org.scu.spark.scheduler.cluster.SparkDeploySchedulerBackend
import org.scu.spark.scheduler.{DAGScheduler, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * 1.spark的主要入口。spark用于连接集群，获取Executor资源。
 * 2.Spark运算流程：
 * sparkContext创建RDD，触发action，通过DAGScheduler形成DAG，转化成TaskSet，
 * TaskSchedulerImpl通过SparkDeploySchedulerBackend的reviveOffers，向ExecutorBackend发送LaunchTask消息，开始在集群中计算
 * Created by bbq on 2015/11/10
 */
class SparkContext(sparkConf: SparkConf) extends Logging {

  logInfo(s"Running Spark version $SPARK_VERSION")

  private var _conf: SparkConf = _
  private var _env: SparkEnv = _

  private[spark] def conf: SparkConf = _conf

  def getConf: SparkConf = conf.clone

  private[spark] def env: SparkEnv = _env

  @volatile private var _dagScheduler: DAGScheduler = _
  private var _schedulerBackend : SchedulerBackend = _
  private var _taskScheduler : TaskScheduler = _
  private var _executorMemoy :Int = _

  /**传给Env的参数*/
  private[spark] val executorEnvs = mutable.HashMap[String,String]()

  def masterHost :String = _conf.get("spark.master.host")
  def masterPort :Int = _conf.getInt("spark.master.port")
  def appName : String = _conf.get("spark.app.name")

  /** 下一个RDD的ID */
  val nextRddId = new AtomicInteger(0)

  private[spark] def newRddId() = nextRddId.getAndIncrement()

  /** 下一个Shuffle的ID */
  val nextShuffleId = new AtomicInteger(0)

  private[spark] def newShuffleId() = nextShuffleId.getAndIncrement()

  private def dagScheduler: DAGScheduler = _dagScheduler
  private def dagScheduler_=(ds: DAGScheduler): Unit = {
    _dagScheduler = ds
  }

  private[spark] def taskScheduler : TaskScheduler = _taskScheduler
  private[spark] def taskScheduler_=(ts:TaskScheduler) = {
    _taskScheduler = ts
  }

  /** 每一个线程都都维护一个本地Properties，并且父进程创建子进程时，会深拷贝一份，防止修改干扰 */
  protected[spark] val localProperties = new InheritableThreadLocal[Properties] {
    override def childValue(parentValue: Properties): Properties = {
      SerializationUtils.clone(parentValue)
    }

    override def initialValue(): Properties = new Properties()
  }

  private[spark] def createSparkEnv(
                                   conf:SparkConf
                                   //TODO isLocal
                                   //TODO listerBus
                                     ):SparkEnv={
    SparkEnv.createDriverEnv(conf,2)
  }

  private[spark] def executorMemory:Int= _executorMemoy

  /**
   * ***重要：系统初始化全部代码：建立DAGScheduler，TaskScheduler等
   */
  _conf = sparkConf.clone
  _env = createSparkEnv(conf)
  SparkEnv.env = _env

  _executorMemoy = _conf.getInt("spark.executor.memory")

  val (sched,ts) = SparkContext.createTaskScheduler(this,RpcAddress(masterHost,masterPort))
  _schedulerBackend = sched
  _taskScheduler = ts
  _dagScheduler = new DAGScheduler(this)

  _taskScheduler.start()



  /**
   * 根据本地数据生成分布式数据片
   */
  def parallelize[T](seq: Seq[T], numSlices: Int = 3) = {
    new ParallelCollectionRDD[T](this, seq, numSlices)
  }

  /**
   * 对所有的partition进行计算
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, rdd.partitions.indices)
  }

  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U, partitions: Seq[Int]): Array[U] = {
    runJob[T,U](rdd, (ctx: TaskContext, it: Iterator[T]) => func(it), partitions)
  }

  /**
   * 计算部分Partition，并将每个Partition的结果存入Array
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U, partitions: Seq[Int]): Array[U] = {
    logDebug("runjob")
    val results = new Array[U](partitions.size)
    runJob[T,U](rdd, func, partitions, (index, res) => results(index) = res)
    results
  }

  /**
   * 这里是所有action的入口，通过resultHandler返回结果
   */
  def runJob[T, U](
                    rdd: RDD[T],
                    func: (TaskContext, Iterator[T]) => U,
                    partitions: Seq[Int],
                    resultHandler: (Int, U) => Unit): Unit = {
    dagScheduler.runJob(rdd, func, partitions, resultHandler, localProperties.get())
  }


}

object SparkContext {

  /** 创建负责任务调度的TaskScheduler及其对应的backend */
  private def createTaskScheduler(sc: SparkContext, master: RpcAddress): (SchedulerBackend, TaskScheduler) = {
    val scheduler = new TaskSchedulerImpl(sc)
    val backend = new SparkDeploySchedulerBackend(scheduler, sc, master)
    scheduler.initialize(backend)
    (backend, scheduler)
  }


  private[spark] val DRIVER_IDENTIFIER = "driver"

  def main(args: Array[String]): Unit = {
    println(System.getProperty("java.io.tmpdir"))
    val conf = new SparkConf()
    conf.set("spark.executor.port","7656")
    val sc = new SparkContext(conf)
    val array: Array[Int] = Array(1, 2, 3, 4, 6, 7, 8, 9, 10)
    val a: ParallelCollectionRDD[Int] = sc.parallelize(array, 10)
    a.map(_ * 2)
    println(a.count())
    Thread.sleep(1000000)
  }
}