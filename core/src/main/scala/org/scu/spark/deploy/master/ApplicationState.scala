package org.scu.spark.deploy.master

/**
 * Created by bbq on 2015/12/20
 */
private[master] object ApplicationState extends Enumeration{
  type ApplicationState = Value

  val WAITING,RUNNING,FINISHED,FAILD,KILLED,UNKOWN = Value

  val MAX_NUM_RETRY = 10

}
