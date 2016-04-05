package org.scu.spark.scheduler

/**
 * Created by bbq on 2016/4/5
 */
object SchedulingMode extends Enumeration{
  type SchedulingMode = Value
  val FIFO,FAIR,NONE = Value
}
