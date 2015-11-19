package org.scu.spark.scheduler

/**
 * 用等待阻塞的方式获取计算结果：
 * 1.若任务未完成，则调用wait
 * 2.当有一个Task完成，则notify，并将完成的任务数加一
 * Created by bbq on 2015/11/19
 */
private[spark] class JobWaiter[T](
                                 dAGScheduler: DAGScheduler,
                                 val jobId:Int,
                                 totalTask:Int,
                                 resultHandler:(Int,T)=>Unit
                                   ) extends JobListerner{
  private var finishedTasks = 0

  //如果totalTask的个数为0，则直接成功
  private var _jobFinished = totalTask == finishedTasks

  private var jobResult:JobResult = if(_jobFinished) JobSucceeded else null

  override def taskSucceeded(index: Int, result: Any): Unit = synchronized{
    if(_jobFinished){
      throw new UnsupportedOperationException("taskSucceeded called on a finished JobWaiter")
    }
    //真正返回结果的地方
    resultHandler(index,result.asInstanceOf[T])
    finishedTasks += 1
    if(finishedTasks==totalTask){
      _jobFinished = true
      jobResult = JobSucceeded
      this.notifyAll()
    }
  }

  override def jobFailed(exception: Exception): Unit = {
    _jobFinished = true
    jobResult = JobFailed(exception)
    this.notifyAll()
  }

  def awaitResult():JobResult=synchronized{
    while(!_jobFinished)
      this.wait()
    jobResult
  }
}
