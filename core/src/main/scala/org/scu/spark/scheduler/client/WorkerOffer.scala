package org.scu.spark.scheduler.client

/**
 * Created by bbq on 2016/4/6
 */
private[spark] case class WorkerOffer(executorId:String,host:String,cores:Int)
