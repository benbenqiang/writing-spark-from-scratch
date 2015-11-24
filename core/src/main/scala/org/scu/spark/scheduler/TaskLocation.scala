package org.scu.spark.scheduler

/**
 * Created by bbq on 2015/11/24
 */
private[spark] sealed trait TaskLocation {
  def host:String
}

private[spark]
case class ExecutorCacheTaskLocation(override val host:String,executorId:String)extends TaskLocation{
  override def toString: String = s"${TaskLocation.executorLocationTag}${host}_$executorId"
}


private[spark] object TaskLocation{

  val executorLocationTag = "executor_"

  def apply(host:String,executorId:String):TaskLocation={
    new ExecutorCacheTaskLocation(host,executorId)
  }
}