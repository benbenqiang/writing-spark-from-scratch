package org.scu.spark.scheduler

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue

import org.scu.spark.{SparkEnv, Logging}
import org.scu.spark.scheduler.TaskLocality._
import org.scu.spark.scheduler.cluster.TaskDescription

import scala.collection.mutable
import scala.collection.mutable._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by  bbq on 2015/12/14
 */
private[spark] class TaskSetManager (
                                    sched:TaskSchedulerImpl,
                                    val taskSet:TaskSet,
                                    val maxTaskFailures:Int
                                      ) extends Schedulable with Logging{

  val conf = sched.sc.conf

  private val EXECUOTR_TASK_BLACKLIST_TIMEOUT = conf.getLong("spark.scheduler.executorTaskBlacklistTime",0L)
  val env = SparkEnv.env
  val ser = env.closureSerializer.newInstance()

  val tasks = taskSet.tasks
  val numTasks=tasks.length
  val copiesRunning = new Array[Int](numTasks)
  val successful = new Array[Boolean](numTasks)
  private val numFailures = new Array[Int](numTasks)
  /**记录每个task失败时的executorID 以及对应的时间*/
  private val failedExecutors = new mutable.HashMap[Int,mutable.HashMap[String,Long]]()

  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)

  override var parent: Pool = _

  override def addSchedulable(schedulable: Schedulable): Unit = ???

  override def schedulableQueue: ConcurrentLinkedQueue[Schedulable] = ???

  //def schedulingMode:SchedulingMode
  override def weight: Int = ???

  val runningTaskSet = new HashSet()

  override def runingTasks: Int = runningTaskSet.size

  /**当这个标志是true时，不会再有新的task运行
    * 当把taskManager kill 掉，或者所有的任务已经完成了，那么就标记成true，用来保留历史运行记录
    * */
  var isZombie = false

  /**每个task根据preferedLocation，会指定相应的Executor，或者host，或者Rack，或者没有
    * 其中String是ExecutorID,ArrayBuffer是tasks的索引位置
    * */
  private val pendingTasksForExecutor = new HashMap[String,ArrayBuffer[Int]]

  private val pendingTasksForHost = new HashMap[String,ArrayBuffer[Int]]

  private val pendingTasksForRack = new HashMap[String,ArrayBuffer[Int]]

  var pendingTasksWithNoPrefs = new ArrayBuffer[Int]

  val allPendingTasks = new ArrayBuffer[Int]

  val taskInfos = new mutable.HashMap[Long,TaskInfo]()

  var myLocalityLevels = computeValidLocalityLevels()

  override def checkSpeculatableTasks(): Boolean = ???

  override var name: String = "TaskSet_"+taskSet.stageId.toString

  override def removeSchedulable(schedulable: Schedulable): Unit = ???

  override def priority: Int = ???

  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = ???

  override def minShare: Int = ???

  override def getSchedulableByName(name: String): Schedulable = ???

  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit = ???

  override def stageId: Int = ???

  /**获取不同locality所相应的延迟调度时间*/
  private def getLocalWait(level:TaskLocality.TaskLocality) : Long = {
    /**
     * 默认所有locatlity等级在未满足运行条件时，所等待的时间
     * 如果为0说明不会因为不满足locality而延迟执行
     * */
    val defaultWait = conf.getInt("spark.locality.wait",3000)
    val localityWaitKey = level match {
      case TaskLocality.PROCESS_LOCAL => "spark.locality.wait.process"
      case TaskLocality.NODE_LOCAL => "spark.locality.wait.node"
      case TaskLocality.RACK_LOCAL => "spark.locality.wait.rack"
      case _ => null
    }
    if(localityWaitKey != null){
      conf.getInt(localityWaitKey,defaultWait).toLong
    }else{
      0L
    }
  }

  /**
   * 计算在当前TaskManager中已经提交的task里，每个task有自己的preferedlocation
   * 根据本地性等级，返回所有已满足的本地性等级，从ProcessLocal开始优先判断是否需要延迟执行
   * 比如，是个task中，有一个分配满足ProcessLocal，其他的都是Any，那么先执行ProcessLocal
   * 其他的任务根据配置等待一段时间，然后再运行，此时之前被占用的资源可能会被释放
    * */
  private def computeValidLocalityLevels():Array[TaskLocality.TaskLocality]={
    import TaskLocality.{PROCESS_LOCAL,NODE_LOCAL,NO_PREF,RACK_LOCAL,ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]

    if(pendingTasksForExecutor.nonEmpty && getLocalWait(PROCESS_LOCAL) != 0 &&
      pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive)){
      levels += PROCESS_LOCAL
    }
    if(pendingTasksForHost.nonEmpty && getLocalWait(NODE_LOCAL) != 0 &&
    pendingTasksForHost.keySet.exists(sched.hasExecuotrsAliveOnHost)){
      levels += NODE_LOCAL
    }
    if(pendingTasksWithNoPrefs.nonEmpty)
      levels += NO_PREF
    if(pendingTasksForRack.nonEmpty && getLocalWait(RACK_LOCAL) != 0 &&
      pendingTasksForRack.keySet.exists(sched.hasHostAliveOnRack))
      levels += RACK_LOCAL
    levels += ANY

    logDebug("Valid locality levels for"+ taskSet +":"+ levels.mkString(","))
    levels.toArray
  }

  /**若一个task在一个executor上已经失败过，并且失败的时间小于规定的时间，那么不运行*/
  private def executorIsBlacklisted(execId:String,taskId:Int):Boolean={
    if(failedExecutors.contains(taskId)){
      val failed = failedExecutors.get(taskId).get
      return failed.contains(execId) && System.currentTimeMillis() - failed.get(execId).get < EXECUOTR_TASK_BLACKLIST_TIMEOUT
    }
    false
  }

  private def getPendingTasksForExecutor(executorId:String):ArrayBuffer[Int]={
    pendingTasksForExecutor.getOrElse(executorId,ArrayBuffer())
  }

  private def getPendingTaskForHost(host:String):ArrayBuffer[Int]={
    pendingTasksForHost.getOrElse(host,ArrayBuffer())
  }

  private def getPendingTaskForRack(rack:String):ArrayBuffer[Int]={
    pendingTasksForRack.getOrElse(rack,ArrayBuffer())
  }

  /**
    * 虽然list 是个buffer ，其实这里实现的是队列的效果，从头插入，从尾提出，之所以使用array是因为index的需要
   * 结果返回task在tasks中的索引位置
    */
  private def dequeueTaskFromList(execId:String,list:ArrayBuffer[Int]):Option[Int]={
    for(indexOffset <- list.size-1 to 0){
      val index = list(indexOffset)
      if(!executorIsBlacklisted(execId,index)){
        /**这里是lazy移除，当task运行完成或者已经运行，或从当前这个列表里一处，在下个判断语句不会被运行*/
        list.remove(indexOffset)
        /**当前没有运行切没有没有运行成功*/
        if(copiesRunning(index) == 0 && !successful(index)){
          return Some(index)
        }
      }
    }
    None
  }

  /**
    *  为每一个worker资源分配一个task，根据worker的locality level 进行分配
   *  优先选择PROCESS_LOCAL,其他的依次判断
   *  @return (task index,TaskLocality,is speculateive)
    */
  private def dequeueTask(execId:String,host:String,maxLocality:TaskLocality.Value)
    :Option[(Int,TaskLocality.Value,Boolean)]={
    /**优先运行PROCESS_LOCAL的task*/
    for(index <- dequeueTaskFromList(execId,getPendingTasksForExecutor(execId))){
       return Some((index,TaskLocality.PROCESS_LOCAL,false))
     }
    /**根据maxLocality的限制来查看是否执行除PROCESS_LOCAL外的task*/
    if(TaskLocality.isAllowed(maxLocality,TaskLocality.NODE_LOCAL))
      for(index <- dequeueTaskFromList(execId,getPendingTaskForHost(host)))
        return Some((index,TaskLocality.NODE_LOCAL,false))

    if(TaskLocality.isAllowed(maxLocality,TaskLocality.NO_PREF))
      for(index <- dequeueTaskFromList(execId,pendingTasksWithNoPrefs))
        return Some((index,TaskLocality.PROCESS_LOCAL,false))

    if(TaskLocality.isAllowed(maxLocality,TaskLocality.RACK_LOCAL)){
      for{
        rack <- sched.getRackForHost(host)
        index <- dequeueTaskFromList(execId,getPendingTaskForRack(rack))
      }{
        return Some((index,TaskLocality.RACK_LOCAL,false))
      }
    }
    /**实在不行最后从所有pending的task中以ANY方式运行*/
    if(TaskLocality.isAllowed(maxLocality,TaskLocality.ANY)){
      for(index <- dequeueTaskFromList(execId,allPendingTasks)){
        return Some((index,TaskLocality.ANY,false))
      }
    }

    //TODO dequeueSpeculativeTask
    //TODO 当所有任务都完成的时候，尝试对正在运行的rask进行推测重新运行
    None
  }

  /**
   *  给定一个executor Id ，为这个Exeucutor分配task
    */
  def resourceOffer(
                     execId:String,
                     host:String,
                     maxLocality:TaskLocality
                     ):Option[TaskDescription]={
    if(!isZombie){
      val curTime = System.currentTimeMillis()

      var allowedLocality = maxLocality

      /**更新maxLocality*/
      if(maxLocality != TaskLocality.NO_PREF){
        //TODO getAllowedLocalityLevel
      }

      /**为一个executor根据本地行分配task*/
      dequeueTask(execId,host,allowedLocality) match {
        case Some((index,taskLocality,speculative)) =>
          val task = tasks(index)
          val taskId = sched.newTaskId()
          copiesRunning(index) += 1
          val attemptNum = taskAttempts(index).size
          /**将task 和executorId 组合成TaskInfo*/
          val info = new TaskInfo(taskId,index,attemptNum,curTime,execId,host,taskLocality,speculative)
          taskInfos(taskId) = info
          taskAttempts(index) = info :: taskAttempts(index)
          //TODO Update locality level
          val startTime = System.currentTimeMillis()
          val serializedTask:ByteBuffer=try{
            Task.serializeWithDependencies(task,sched.sc.addedFiles,sched.sc.addedJars,ser)
          }
      }
      }

    ???
    }

  def executorAdded(): Unit ={
    ???
  }
}
