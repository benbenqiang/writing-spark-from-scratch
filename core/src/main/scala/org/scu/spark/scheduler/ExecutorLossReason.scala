package org.scu.spark.scheduler

/**
 * Created by bbq on 2016/4/3
 */
private[spark] class ExecutorLossReason(val message:String) extends Serializable{
  override def toString:String = message
}

private[spark] case class ExecutorExited(exitCode:Int,exitCauseByApp:Boolean,reason:String)
extends ExecutorLossReason(reason)

private[spark] object ExecutorKilled extends ExecutorLossReason("Exeutor killed by driver")

private[spark] object LossReasonPending extends ExecutorLossReason("Pending loss reason")

private[spark] case class SlaveLost(_message:String = "Slave lost") extends ExecutorLossReason(_message)