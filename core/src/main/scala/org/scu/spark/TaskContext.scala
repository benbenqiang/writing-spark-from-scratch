package org.scu.spark

/**
 * 每个执行在Partition上的线程，使用的Task上下文,通过ThreadLocal为每个线程维护一套Context
 * Created by bbq on 2015/11/19
 */
object TaskContext{

  def get() = taskContext.get()

  val taskContext = new ThreadLocal[TaskContext]

  def setTaskContext(tc:TaskContext)=taskContext.set(tc)

}

class TaskContext extends Serializable{

}
