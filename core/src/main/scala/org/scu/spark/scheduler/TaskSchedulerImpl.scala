package org.scu.spark.scheduler

import java.util.{TimerTask, Timer}

import org.scu.spark.executor.TaskMetrics
import org.scu.spark.rdd.SparkException
import org.scu.spark.scheduler.client.WorkerOffer
import org.scu.spark.scheduler.cluster.TaskDescription
import org.scu.spark.storage.BlockManagerID
import org.scu.spark.{Logging, SparkContext}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random


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

  /**每一个Executor所运行的Task总数*/
  private val executorIdToTaskCount = new HashMap[String,Int]
  /**每个Host上所运行的Executor集合*/
  protected val executorsByHost = new HashMap[String,HashSet[String]]
  /**每个机架上的host列表*/
  protected val hostsByRack = new HashMap[String,HashSet[String]]
  /**每个Executor所在机器的Host*/
  protected val executorIdToHost = new HashMap[String,String]


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

  /**初始化TaskSchedulerImple,根据不同的调度策略（FIFO，FAIR）构建调度池*/
  def initialize(backend:SchedulerBackend) ={
    _backend = backend
    rootPool = new Pool("",schedulingMode,0,0)
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FIFOSchedulableBuilder(rootPool)
      }
    }
    schedulableBuilder.buildPools()
  }

  override def start(): Unit = {
    _backend.start()
  }

  override def applicationAttemptId(): Option[String] = ???

  override def stop(): Unit = ???

  override def defaultParallelism(): Int = ???

  override def canelTasks(stageId: Int, interruptThread: Boolean): Unit = ???

  /**DAGScheduler 通过这个方法将taskSet交给TaskScheduler*/
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

  /**
   * 根据backend传来的workerOffers,将task分配给不同的worker，
    * 返回TaskDescription给Backend
    * Backend 通过TaskDescription与Executor通信，进行任务的分发运行
    * */
  def resourceOffers(offers:Seq[WorkerOffer]) : Seq[Seq[TaskDescription]]=synchronized{
    var newExecAvail = false
    for( o <- offers){
      executorIdToHost(o.executorId) = o.host
      executorIdToTaskCount.getOrElseUpdate(o.executorId,0)
      /**有新的host加入*/
      if(!executorsByHost.contains(o.host)){
        executorsByHost(o.host) = new HashSet[String]
        //TODO executorAdded
        newExecAvail = true
      }
      for(rack <- getRackForHost(o.host)){
        hostsByRack.getOrElseUpdate(rack,new HashSet[String]) += o.host
      }
    }

    /**对当前已经提交给TaskScheduler的所有可运行任务进行排序，开始计算*/
    val shuffledOffers = Random.shuffle(offers)
    val tasks = shuffledOffers.map(o=>new ArrayBuffer[TaskDescription](o.cores))
    val availableCpus = shuffledOffers.map(_.cores).toArray
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for(taskSet <- sortedTaskSets){
      logDebug(s"parentName: ${taskSet.parent.name} ,name: ${taskSet.name} , runningTasks: ${taskSet.runingTasks}")
      if(newExecAvail){
        taskSet.executorAdded()
      }
    }

    /**在对任务进行排序之后，需要考虑那些task要在哪个worker上运行，通过preferredLocatlity,
      * 优先级顺序是： PROCESS_LOCAL,NODE_LOCAL,NO_PREF,RACK_LOCAL,ANY
      * */

    ???
  }

  def getRackForHost(value:String):Option[String] = None

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
