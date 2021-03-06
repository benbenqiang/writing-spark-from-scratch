package org.scu.spark.storage

import akka.actor.ActorRef
import org.scu.spark.storage.BlockManagerMessage.UpdateBlockInfo
import org.scu.spark.{Logging, SparkConf}
import akka.pattern.ask
import org.scu.spark.rpc.akka.AkkaRpcEnv
import org.scu.spark.util.RpcUtils
/**
 * driver和executor都有一个Master，该类主要是对BlockManagerMasterEndPoint的操作进行包装
 * Created by bbq on 2016/5/9
 */
class BlockManagerMaster(
                        var driverEndpoint:ActorRef,
                        conf:SparkConf,
                        isDriver:Boolean
                          ) extends  Logging{
  def updateBlockInfo(
                      blockManagerId:BlockManagerId,
                      blockId:BlockId,
                      storageLevel: StorageLevel,
                      memSize:Long,
                      diskSize:Long):Boolean = {
    implicit val timeout = RpcUtils.askRpcTimeout(conf)
    val res = AkkaRpcEnv.askSyn[Boolean](driverEndpoint,UpdateBlockInfo(blockManagerId,blockId,storageLevel,memSize,diskSize),conf)
    logDebug(s"Updated info of block $blockId")
    res
  }

}

private[spark] object BlockManagerMaster {
  val DRIVER_ENDPOINT_NAME = "blockManagerMaster"
}
