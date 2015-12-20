package org.scu.spark.deploy

/**
 * schedulerbackend 通过appClient 向 Master 传递应用的信息
 * Created by bbq on 2015/12/17
 */
private[spark] case class ApplicationDescription(
                                                name:String,
                                                maxCores:Option[Int],
                                                memoryPerExecutorMB:Int,
                                                coresPerExecutor:Option[Int]=None
                                                //TODO a lot
                                                  ) {

}
