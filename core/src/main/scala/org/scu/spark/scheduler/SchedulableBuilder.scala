package org.scu.spark.scheduler

import java.util.Properties

import org.scu.spark.Logging

/**
 * 构建schdulable的树结构
 * Created by bbq on 2016/4/5
 */
private[spark] trait SchedulableBuilder {
  def rootPool:Pool

  def buildPools()

  def addTaskSetManager(manager:Schedulable,properties:Properties)
}

private[spark] class FIFOSchedulableBuilder(val rootPool:Pool) extends SchedulableBuilder with Logging{
  override def buildPools(): Unit = {}

  override def addTaskSetManager(manager: Schedulable, properties: Properties): Unit ={
    rootPool.addSchedulable(manager)
  }
}