package org.scu.spark.deploy.master

/**
 * Created by bbq on 2015/12/18
 */
private[master] object WorkerState extends Enumeration{
  type WorkerState = Value

  val ALIVE,DEAD,DECOMMISSIONED,UNKNOWN = Value
}
