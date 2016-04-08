package org.scu.spark.scheduler

import java.util.{TimerTask, Timer}

import org.scu.spark.executor.TaskMetrics
import org.scu.spark.rdd.SparkException
import org.scu.spark.scheduler.client.WorkerOffer
import org.scu.spark.scheduler.cluster.TaskDescription
import org.scu.spark.storage.BlockManagerID
import org.scu.spark.{Logging, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 * 通过SchedulerBackend在不同的集群上调度task。
 * 与具体的task运行由不同的backend负责（standalone,yarn,messos），taskschedulerimple 负责调度相关。
 *
 * Created by bbq on 2015/12/11
 */
private[spark] class TaskSchedulerImpl(
                                      val sc:SparkContext,
                                      val maxTaskFailures:Int,
                                      isLocal:Boolean = false
                                        ) extends TaskScheduler with Logging{

  def this(sc:SparkContext) = this(sc,sc.conf.getInt("spark.task.maxFailures",4))

  val conf = sc.conf

  /**通过StageId 和 attempt的次数定位TaskSetManager*/
  private val taskSetsByStagewIdAndAttempt = new HashMap[Int,HashMap[Int,TaskSetManager]]

  @volatile private var hasReceivedTRask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  val STARVATION_TIMEOUT_MS = conf.getInt("spark.starvation.timeout",15000)

  var _backend : SchedulerBackend = _

  var schedulableBuilder : SchedulableBuilder = null
  var rootPool:Pool = null
  private val schedulingModeConf = conf.get("spark.scheduler.mode","FIFO")
  val schedulingMode = try{SchedulingMode.withName(schedulingModeConf.toUpperCase())
  } catch {
    case e:NoSuchElementException=>
      throw new SparkException("unrecognized spark.scheduler.mode :"+schedulingModeConf)
  }


  def initialize(backend:SchedulerBackend) ={
    _backend = backend
    rootPool = new Pool("",schedulingMode,0,0)
    schedulableBuilder = new FIFOSchedulableBuilder(rootPool)

    schedulableBuilder.buildPools()
  }
  override def start(): Unit = {
    _backend.start()
  }

  override def applicationAttemptId(): Option[String] = ???

  override def stop(): Unit = ???

  override def defaultParallelism(): Int = ???

  override def canelTasks(stageId: Int, interruptThread: Boolean): Unit = ???

  override def submitTasks(taskSet: TaskSet) = {
    val tasks = taskSet.tasks
    logInfo("Adding task set "+taskSet.id + " with " + tasks.length + "tasks")
    this.synchronized{
      val manager = createTaskSetManager(taskSet,maxTaskFailures)
      val stage = taskSet.stageId
      val stageTaskSets = taskSetsByStagewIdAndAttempt.getOrElseUpdate(stage,new HashMap[Int,TaskSetManager])
      stageTaskSets(taskSet.stageAttemptId)=manager
      //TODO conflickt TaskSet
      schedulableBuilder.addTaskSetManager(manager,manager.taskSet.properties)

      if(!isLocal && !hasReceivedTRask){
        starvationTimer.scheduleAtFixedRate(new TimerTask(){
          override def run(): Unit = {
            /**TaskSchedulerImple 在 resourceOffers 的时候是否成功获取到了资源*/
            if(!hasLaunchedTask){
              logWarning("Initial job has not accepted any resources;" +
              "check your cluster UI to ensure that worker are registered " +
              "and have sufficient resources"
              )
            }else{
              this.cancel()
            }
          }
        },STARVATION_TIMEOUT_MS,STARVATION_TIMEOUT_MS)
      }
      hasReceivedTRask = true
    }
    /**通知backend中的driver，开始获取worker资源*/
    _backend.reviveOffers()
  }


  private[scheduler] def createTaskSetManager(
                                             taskSet: TaskSet,
                                             maxTaskFailures:Int
                                               ):TaskSetManager={
    new TaskSetManager(this,taskSet,maxTaskFailures)
  }
  override def executorLost(executorId: String, reason: String): Unit = ???

  override def executorHeartbeatReceived(execId: String, taskMetrics: Array[(Long, TaskMetrics)], blockManagerID: BlockManagerID): Boolean = ???

  override def setDAGScheduler(dAGScheduler: DAGScheduler): Unit = ???
}
