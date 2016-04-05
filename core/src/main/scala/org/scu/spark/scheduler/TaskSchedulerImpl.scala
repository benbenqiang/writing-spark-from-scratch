package org.scu.spark.scheduler

import org.scu.spark.executor.TaskMetrics
import org.scu.spark.rdd.SparkException
import org.scu.spark.storage.BlockManagerID
import org.scu.spark.{Logging, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 * 通过SchedulerBackend在不同的集群上调度task
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

    }
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
