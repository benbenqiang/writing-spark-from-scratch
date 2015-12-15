package org.scu.spark.scheduler.client

/**
 * appclient的回掉方法。当与集群通讯时所发上的事件
 * Created by bbq on 2015/12/15
 */
private[spark] trait AppClientListener {
  def connected(appId:String) : Unit

  /**
   * disconnection是一个短暂的状态，我们会恢复到一个新的Master上
   */
  def disconnected():Unit

  /**
   * 当一个应用发生了不可恢复的错误是调用
   * @param reason
   */
  def dead(reason:String):Unit

  def executorAdded (fullId:String,workerId:String,hostPort:String,cores:Int,memory:Int)

  def executorRemoved(fullId:String,message:String,exitStatus:Option[Int]):Unit

}
