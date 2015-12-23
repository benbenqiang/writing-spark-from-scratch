package org.scu.spark.deploy.worker

/**
 * Created by bbq on 2015/12/23
 */
private[deploy] object ExecutorState extends Enumeration{
  val LAUCHING,RUNNING,KILLED,FAILED,LOST,EXITED = Value

  type ExectutorState = Value

  def isFinished(state:ExectutorState):Boolean=Seq(KILLED,FAILED,LOST,EXITED).contains(state)

}
