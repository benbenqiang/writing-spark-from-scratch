package org.scu.spark.scheduler.cluster

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef, Props}
import org.scu.spark.Logging
import org.scu.spark.rpc.akka.{RpcAddress, AkkaUtil, AkkaRpcEnv}
import org.scu.spark.scheduler.client.WorkerOffer
import org.scu.spark.scheduler.cluster.CoarseGrainedClusterMessage._
import org.scu.spark.scheduler.{SchedulerBackend, TaskSchedulerImpl}
import org.scu.spark.util.{Utils, ThreadUtils}

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer

/**
 * 一个粗粒度的调度器，为一个job分配固定的资源，为多个task共享，
 * 而不是运行完一个归还资源，并且为新Task重新分配资源。
 * Created by bbq on 2015/12/11
 */
private[spark] class CoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, val rpcEnv: AkkaRpcEnv)
  extends SchedulerBackend {
  /**记录总共的CPU个数*/
  var totalCoreCount = new AtomicInteger(0)
  /**总共的Executor个数*/
  var totalRegisterExecutors = new AtomicInteger(0)

  val conf = scheduler.sc.conf

  /** dirver程序的RPC对象 */
  var driverEndPoint: ActorRef = _

  /**根据executorID对Executor信息进行存储*/
  private val executorDataMap = new HashMap[String,ExecutorData]
  /**applicaiton请求executor的个数未满足的个数*/
  private var numPendingExecutors = 0
  /**需要被移除的executor,知道原因的*/
  private val executorPendingToRemove = new HashMap[String,Boolean]
  /**需要被移除的executor，未知原因的*/
  private val executorsPendingLossReason= new HashSet[String]

  override def start(): Unit = {
    val properties: ArrayBuffer[(String, String)] = new ArrayBuffer[(String, String)]

    /** 从conf中读取properties */
    for ((key, value) <- scheduler.sc.conf.getAll) {
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }
    rpcEnv.doCreateActor(Props(new DriverEndPoint( rpcEnv, properties)), CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
  }

  override def stop(): Unit = ???

  override def defaultParallelism(): Int = ???

  override def reviveOffers(): Unit = ???


  class DriverEndPoint(val rpcEnv: AkkaRpcEnv, sparkProperties: ArrayBuffer[(String, String)]) extends Actor with Logging {

    protected val addressToExecutorId = new HashMap[RpcAddress,String]

    private val reviveThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-revive-thread")

    override def preStart(): Unit = {
      val reviveIntervalMs = conf.getInt("spark.scheduler.revive.interval",1) * 1000

      reviveThread.scheduleAtFixedRate(new Runnable{
        override def run(): Unit = Utils.tryLogNonFatalError{
          self ! ReviveOffers
        }
      },0,reviveIntervalMs,TimeUnit.MICROSECONDS)
    }

    override def receive: Receive = {
      case RetrieveSparkProps =>
        sender() ! sparkProperties
      case RegisterExecutor(executorId,executorRef,cores,logUrls) =>
        if(executorDataMap.contains(executorId)){
          sender() ! RegisterExecutorFailed("Duplicate executor ID : "+ executorId)
        }else{
          val executorAddress = AkkaUtil.getRpcAddressFromActor(sender())
          logInfo(s"Registered executor $executorRef ($executorAddress) with ID $executorId")
          addressToExecutorId(executorAddress)= executorId
          totalCoreCount.addAndGet(cores)
          totalRegisterExecutors.addAndGet(1)
          val data = new ExecutorData(executorRef,executorAddress,executorAddress.host,cores,cores,logUrls)
          CoarseGrainedSchedulerBackend.this.synchronized{
            executorDataMap.put(executorId,data)
            if(numPendingExecutors >0){
              numPendingExecutors -= 1
              logDebug(s"Decrementd number of pending execuotrs $numPendingExecutors left")
            }
          }
          sender() ! RegisteredExectutor(executorAddress.host)
          //TODO SparkListener
          makeOffers()
        }

      case ReviveOffers =>
        makeOffers()

    }

    private def makeOffers() ={
      val activeExecutors = executorDataMap.filterKeys(executorIsAlive)
      val workOffers = activeExecutors.map{
        case (id,executorData)=>
          new WorkerOffer(id,executorData.executorHost,executorData.freeCores)
      }.toSeq
      /**如何调度交给taskScheduler，具体任务分发的细节，交给Backend*/
//      launchTasks
    }

    private def executorIsAlive(executorId:String):Boolean=synchronized{
      !executorPendingToRemove.contains(executorId) && !executorsPendingLossReason.contains(executorId)
    }

    private def launchTasks(tasks :Seq[Seq[TaskDescription]]): Unit ={
      /**首先将task扁平化，task之间是可并行的没有依赖关系*/
      for (task <- tasks.flatten){

      }
    }
  }

}

private[spark] object CoarseGrainedSchedulerBackend {
  val ENDPOINT_NAME = "CoraseGrainedScheduler"
}

