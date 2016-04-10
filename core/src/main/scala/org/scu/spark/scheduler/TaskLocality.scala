package org.scu.spark.scheduler

/**
 * Created by bbq on 2016/4/10
 */
object TaskLocality extends Enumeration{
  val PROCESS_LOCAL,NODE_LOCAL,NO_PREF,RACK_LOCAL,ANY=Value

  type TaskLocaliry = Value
}
