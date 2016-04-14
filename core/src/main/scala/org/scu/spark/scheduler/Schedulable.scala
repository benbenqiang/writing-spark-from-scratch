package org.scu.spark.scheduler

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

/**
 *
 * Created by bbq on 2016/4/3
 */
private[spark] trait Schedulable {
  var parent:Pool
  def schedulableQueue:ConcurrentLinkedQueue[Schedulable]
  //def schedulingMode:SchedulingMode
  def weight:Int
  def minShare:Int
  def runningTasks:Int
  def priority:Int
  def stageId:Int
  def name:String

  def addSchedulable(schedulable: Schedulable):Unit
  def removeSchedulable(schedulable: Schedulable):Unit
  def getSchedulableByName(name:String):Schedulable
  def executorLost(executorId:String,host:String,reason:ExecutorLossReason):Unit
  def checkSpeculatableTasks():Boolean
  def getSortedTaskSetQueue:ArrayBuffer[TaskSetManager]
}
