package org.scu.spark.storage

import akka.actor.ActorRef
import org.scu.spark.{Logging, SparkConf}

/**
 * driver和executor都有一个Master，该类主要是对BlockManagerMasterEndPoint的操作进行包装
 * Created by bbq on 2016/5/9
 */
class BlockManagerMaster(
                        var driverEndpoint:ActorRef,
                        conf:SparkConf,
                        isDriver:Boolean
                          ) extends  Logging{


}

private[spark] object BlockManagerMaster {
  val DRIVER_ENDPOINT_NAME = "blockManagerMaster"
}
