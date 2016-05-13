package org.scu.spark.storage

import akka.actor.Actor
import akka.actor.Actor.Receive
import org.scu.spark.{Logging, SparkConf}
import org.scu.spark.rpc.akka.AkkaRpcEnv

/**
 * BlockManager的主节点，维护block信息，在集群中删除和添加blcok
 * Created by bbq on 2016/5/9
 */
private[spark] class BlockManagerMasterEndpoint(
                                                 val rpcEnv:AkkaRpcEnv,
                                                 val isLocal : Boolean,
                                                 conf : SparkConf
                                                   ) extends  Actor with  Logging{
  override def receive: Receive = ???
}

/**Block缓存的状态*/
case class BlockStatus(storageLevel: StorageLevel,memSize:Long,diskSize:Long){
  def isCached:Boolean = memSize + diskSize > 0
}