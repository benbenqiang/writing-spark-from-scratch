package org.scu.spark.scheduler

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import akka.remote.FailureDetector.Clock
import org.apache.commons.lang3.SerializationUtils
import org.scu.spark._
import org.scu.spark.rdd.RDD

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.control.NonFatal

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
                                   private[scheduler] val sc: SparkContext,
                                   private[scheduler] val taskScheduler:TaskScheduler
                                   ) extends Logging {

  def this(sc:SparkContext) = this(sc,sc.taskScheduler)

  private val nextJobId = new AtomicInteger(0)
  private val nextStageId = new AtomicInteger(0)

  private[scheduler] val jobIdToStageIds = new mutable.HashMap[Int, mutable.HashSet[Int]]
  private[scheduler] val stageIdToStage = new mutable.HashMap[Int, Stage]
  private[scheduler] val shuffleIdToMapStage = new mutable.HashMap[Int, ShuffleMapStage]()
  private[scheduler] val jobIdToActiveJob = new mutable.HashMap[Int, ActiveJob]


  private[scheduler] val waitingStages = new mutable.HashSet[Stage]
  private[scheduler] val runningStages = new mutable.HashSet[Stage]
  private[scheduler] val failedStages = new mutable.HashSet[Stage]

  private[scheduler] val activeJobs = new mutable.HashSet[ActiveJob]
  /**
   * key是RDD的id，值是每个parition所缓存的地址
   */
  private val cacheLocs = new mutable.HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)


  private def updateJobIdStageIdMaps(jobId:Int,stage:Stage)={
    @tailrec
    def updateJobIdStageIdMapList(stages:List[Stage]):Unit ={
      if(stages.nonEmpty){
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId,new mutable.HashSet[Int]()) += s.id
        val parents = getParentStages(s.rdd,jobId)
        /**父stage中还未包含JobId的，需要更新的stage*/
        val parentsWithoutThisJobId = parents.filter{ ! _.jobIds.contains(jobId)}
        updateJobIdStageIdMapList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapList(List(stage))
  }

  /**
   * 创建一个ResultStage
   * 注：spark中一个有两种stage：ResultStage，和ShuffleMapStage
   */
  private def newResultStage(
                              rdd: RDD[_],
                              func: (TaskContext, Iterator[_]) => _,
                              partitions: Seq[Int],
                              jobId: Int
                              ): ResultStage = {
    val (parentStage: List[Stage], id: Int) = getParentStageAndId(rdd, jobId)
    /** 先生成父stage，最后生成结果stage */
    val stage = new ResultStage(id, rdd, func, partitions, parentStage, jobId)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId,stage)
    stage
  }


  /**
   * 创建一个ShuffleMapStage，通常不直接调用这个方法，而是使用 newOrUsedShuffleStage
   * 注：spark中一个有两种stage：ResultStage，和ShuffleMapStage
   */
  private def newShuffleMapStage(
                                  rdd: RDD[_],
                                  numTasks: Int,
                                  shuffleDep: ShuffleDependency[_, _, _],
                                  firstJobId: Int
                                  ): ShuffleMapStage = {
    val (parentStage: List[Stage], id: Int) = getParentStageAndId(rdd, firstJobId)
    val stage: ShuffleMapStage = new ShuffleMapStage(id, rdd, numTasks, parentStage, firstJobId, shuffleDep)
    stageIdToStage(id) = stage
    ???
    stage
  }

  /**
   * 创建一个ShuffleMapStage
   * 如果这个依赖已经存在，则通过MapOutPutTraker来恢复已经计算好的结果
   */
  private def newOrUsedShuffleStage(
                                     shuffleDep: ShuffleDependency[_, _, _],
                                     firstJobId: Int
                                     ): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val numTasks = rdd.partitions.length
    val stage = newShuffleMapStage(rdd, numTasks, shuffleDep, firstJobId)
    ???
    stage
  }

  /**
   * 获取RDD所有的Stage列表，并且返回最后一个StageID
   */
  private def getParentStageAndId(rdd: RDD[_], firstJobId: Int): (List[Stage], Int) = {
    val parentStage = getParentStages(rdd, firstJobId)
    /** 最后生成末尾stage */
    val id = nextStageId.getAndIncrement()
    (parentStage, id)
  }

  /**
   * 广度遍历rdd的所有依赖，用于生成所有shuffleMapStage 的类似于getParentStage
   **/
  private def getAncestorShuffleDependencies(rdd: RDD[_]): mutable.Stack[ShuffleDependency[_, _, _]] = {
    val parents = new mutable.Stack[ShuffleDependency[_, _, _]]
    val visited = new mutable.HashSet[RDD[_]]()

    val waitingForVisit = new mutable.Stack[RDD[_]]()
    def visit(r: RDD[_]): Unit = {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              if (shuffleIdToMapStage.contains(shufDep.shuffleId))
                parents.push(shufDep)
            case _ =>
          }
          waitingForVisit.push(dep.rdd)
        }
      }
    }
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty)
      visit(waitingForVisit.pop())
    parents
  }

  /**
   * 根据shuffleDep来创建RDD所依赖的上一个ShuffleMapStage
   * 此外，还递归的将所有祖先依赖Stage进行了创建，并加入了shuffleIDToMapStage中
   */
  private def getShuffleMapStage(
                                  shuffleDep: ShuffleDependency[_, _, _],
                                  firstJobId: Int
                                  ): ShuffleMapStage = {
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      /** stage已经创建 */
      case Some(stage) => stage
      case None =>
        getAncestorShuffleDependencies(shuffleDep.rdd).foreach(dep =>
          shuffleIdToMapStage(dep.shuffleId) = newOrUsedShuffleStage(dep, firstJobId)
        )
        /** 先处理父stage，最后在生成最后的stage */
        val stage = newOrUsedShuffleStage(shuffleDep, firstJobId)
        shuffleIdToMapStage(shuffleDep.shuffleId) = stage
        stage
    }
  }


  /**
   * 生成Stage的关键方法：使用广度优先便利获取并生成依赖的父Stage
   */
  private def getParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    /** 返回结果：依赖的父Stage */
    val parents = new mutable.HashSet[Stage]()
    /** 记录遍历过哪些RDD */
    val visited = new mutable.HashSet[RDD[_]]()
    /** 记录需要被遍历的RDD节点 */
    val waitingForVisit = new mutable.Stack[RDD[_]]()

    def visit(r: RDD[_]): Unit = {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              parents += getShuffleMapStage(shufDep, firstJobId)
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty)
      visit(waitingForVisit.pop())
    parents.toList
  }

  /**通过RDD的以来关系，找出出发action的RDD所有以来的stage*/
  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new mutable.HashSet[Stage]()
    val visited = new mutable.HashSet[RDD[_]]()
    val waitingForVisit = new mutable.Stack[RDD[_]]

    def visit(rdd: RDD[_]): Unit = {
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        if (rddHasUncachedPartitions) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getShuffleMapStage(shufDep, stage.firstJobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              case _ =>
                waitingForVisit.push(dep.rdd)
            }
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
   * 获取RDD缓存的地址
   */
  private[scheduler] def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = {
    cacheLocs.synchronized {
      if (!cacheLocs.contains(rdd.id)) {
        /** 若RDD storage level 为None，则缓存链表为空，反之则从block manager中获取 */
        val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
          IndexedSeq.fill(rdd.partitions.length)(Nil)
        } else {
          ???
        }
        cacheLocs(rdd.id) = locs
      }
      cacheLocs(rdd.id)
    }
  }

  private def clearCacheLocs() = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
   * 提交任务，并返回JobWaiter
   */
  def submitJob[T, U](
                       rdd: RDD[T],
                       func: (TaskContext, Iterator[T]) => U,
                       partitions: Seq[Int],
                       resultHandler: (Int, U) => Unit,
                       properties: Properties
                       ): JobWaiter[U] = {

    val jobId = nextJobId.getAndIncrement()
    if (partitions.isEmpty) {
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    val waiter = new JobWaiter[U](this, jobId, partitions.size, resultHandler)

    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    eventProcessLoop.post(JobSubmit(
      jobId, rdd, func2, partitions.toArray, waiter, SerializationUtils.clone(properties)))
    waiter
  }

  /**
   * 计算job，并将结果传给resultHandler
   */
  def runJob[T, U](
                    rdd: RDD[T],
                    func: (TaskContext, Iterator[T]) => U,
                    partitions: Seq[Int],
                    resultHandler: (Int, U) => Unit,
                    properties:Properties): Unit = {
    val start = System.nanoTime()
    logDebug("starting job on "+start)
    val waiter = submitJob(rdd, func, partitions, resultHandler,properties)
    waiter.awaitResult() match {
      case JobSucceeded => logInfo(s"Job ${waiter.jobId},took ${System.nanoTime() - start} s")
      case JobFailed(e) => logInfo(s"Job ${waiter.jobId},took ${System.nanoTime() - start} s")
    }
  }

  /**
   * 处理任务提交消息的操作
   */
  private[scheduler] def handleJobSubmitted(jobId: Int,
                                            finalRDD: RDD[_],
                                            func: (TaskContext, Iterator[_]) => _,
                                            partitions: Seq[Int],
                                            listener: JobListerner,
                                             properties:Properties): Unit = {
    var finalStage: ResultStage = null

    /**
     * 重要：调用newResultStage的时候，不仅生成了最后一个Stage，而且还递归的生成了所有Stage
     * 所以这里需要捕获异常,通知给JobWaiter任务失败了
     **/
    try {
      finalStage = newResultStage(finalRDD, func, partitions, jobId)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception -job " + jobId, e)
        listener.jobFailed(e)
        return
    }
    val job = new ActiveJob(jobId, finalStage, listener,properties)
    clearCacheLocs()
    logInfo("Got job %s with %d output partitions".format(job.jobId, partitions.length))
    logInfo(s"Fianl stage: $finalStage ")
    logInfo("Parents of final stage:" + finalStage.paraents)
    logInfo("Missing parents:" + getMissingParentStages(finalStage))

    val jobSubmissionTime = System.currentTimeMillis()
    jobIdToActiveJob(jobId)=job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray

    submitStage(finalStage)
  }

  private def submitStage(stage: Stage): Unit = {
    /** 提交的stage当前的activejobID */
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        /** 获取所有没有完成的StageID，并从ID最小的开始运行 */
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing:" + missing)

        /** 没有依赖的stage未完成 */
        if (missing.isEmpty) {
          logInfo(s"Submitting $stage (${stage.rdd}),which has no missing parents")
          submitMissingTasks(stage,jobId.get)
        } else {
          for (parent <- missing) {
            submitStage(parent)
          }
          waitingStages += stage
        }
      }
    } else {
      /** TODO : abortStage */
    }
  }

  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobThatUseTage: Array[Int] = stage.jobIds.toArray.sorted
    jobThatUseTage.find(jobIdToActiveJob.contains)
  }

  /**
   * 提交stage的终极方法，只有所有父stage都计算完成才会提交
   */
  private def submitMissingTasks(stage: Stage, jobId: Int): Unit = {
    logDebug(s"submitMissingTasks($stage)")

    stage.pendingPartitons.clear()

    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

    /**
     * 如果internalAccumulator为空，或者全部的partition都需要计算。
     */
    if (stage.internalAccumulators.isEmpty || stage.numPartitions == partitionsToCompute.size) {
      stage.resetInternalAccumulators()
    }

    /** 任务的属性，调度池，任务组，任务描述等 */
    val properties = jobIdToActiveJob(jobId).properties

    runningStages += stage

    //TODO outputCommitCoordinator

    /**
     * 根据partition的信息计算每个task的preferedlocation
     */
    val taskIdToLocation: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id)) }.toMap
        case s: ResultStage =>
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      /** 除了重要的Error如OOM，其他的都不管，会重新尝试stage */
      case NonFatal(e) =>
        //TODO repeat attempt
        ???
    }

    //TODO send stageInfo to SparkListener

    /**
     * 将task序列化后广播出去，分发给每个executor，每个executor反序列化
     * 后得到任务。
     */
    //TODO taskBinaryBytes

    /**
     * 根据stage生成需要计算的tasks
     */
    val tasks: Seq[Task[_]] = try {
      stage match {
        case stage: ShuffleMapStage =>
          partitionsToCompute.map { id =>
            val locs = taskIdToLocation(id)
            val part = stage.rdd.partitions(id)
            new ShuffleMapTask(stage.id, stage.latestInfo.attempteId, part,
              locs, stage.internalAccumulators)
          }
        case stage: ResultStage =>
          partitionsToCompute.map { id =>
            val p = stage.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocation(id)
            new ResultTask(stage.id, stage.latestInfo.attempteId, part,
              locs, id, stage.internalAccumulators)
          }
      }
    } catch {
      case NonFatal(e) =>
        //TODO abortStage
        ???
    }

    /** 向taskScheduler提交任务 ，至此，DAGScheduler已经完成了他的使命*/
    if (tasks.nonEmpty) {
      logInfo(s"Submitting ${tasks.size} missing tasks form $stage (${stage.rdd})")
      stage.pendingPartitons ++= tasks.map(_.partitionId)
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attempteId, jobId, properties))
      stage.latestInfo._submissionTime = Some(System.currentTimeMillis())
    } else {
      //TODO 处理任务运行完毕
    }
  }

  def getPreferredLocs(rdd:RDD[_],partition:Int):Seq[TaskLocation]={
    Seq(TaskLocation("testLocationHost","testExecutorID"))
  }

  eventProcessLoop.start()
}

private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler) extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  override protected def onError(e: Throwable): Unit = {

  }

  override protected def onReceive(event: DAGSchedulerEvent): Unit = {
    doOnReceive(event)
  }

  private def doOnReceive(event: DAGSchedulerEvent) = event match {
    case JobSubmit(jobId, rdd, func, partitions, listerner,properties) =>
      logDebug("EnventProcee receive handleJob Message")
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, listerner,properties)
  }
}
