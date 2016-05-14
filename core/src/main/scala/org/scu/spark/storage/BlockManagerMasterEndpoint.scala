package org.scu.spark.storage

import akka.actor.{ActorRef, Actor}
import org.scu.spark.storage.BlockManagerMessage.UpdateBlockInfo
import org.scu.spark.{Logging, SparkConf}
import org.scu.spark.rpc.akka.AkkaRpcEnv

import scala.collection.mutable

/**
 * BlockManager的主节点，维护block信息，在集群中删除和添加blcok
 * Created by bbq on 2016/5/9
 */
private[spark] class BlockManagerMasterEndpoint(
                                                 val rpcEnv:AkkaRpcEnv,
                                                 val isLocal : Boolean,
                                                 conf : SparkConf
                                                   ) extends  Actor with  Logging{
  private val blockManagerInfo = new mutable.HashMap[BlockManagerId,BlockManagerInfo]

  override def receive: Receive = {
    case _updateBlockInfo @ UpdateBlockInfo(blockManagerId,blockId,storageLevel,memSize,size) =>
      sender() ! updateBlockInfo(blockManagerId,blockId,storageLevel,memSize,size)
      //TODO ListenerBus

  }

  def updateBlockInfo(
                     blockManagerId: BlockManagerId,
                     blockId: BlockId,
                     storageLevel: StorageLevel,
                     memSize:Long,
                     diskSize:Long
                       ): Boolean = {
    if(!blockManagerInfo.contains(blockManagerId)){
      /**分布式环境下，driver的BlockManager重复注册返回true*/
      if(blockManagerId.isDriver && !isLocal)
        return true
      else
        return false
    }
    /**若只是心跳信息，那么就只更新最近时间*/
    if(blockId == null){
      blockManagerInfo(blockManagerId).updateLastSennMs()
      return true
    }

    /**更新block信息*/


???
  }
}

/**Block缓存的状态*/
case class BlockStatus(storageLevel: StorageLevel,memSize:Long,diskSize:Long){
  def isCached:Boolean = memSize + diskSize > 0
}

private[spark] class BlockManagerInfo(
                                     val blockManagerId: BlockManagerId,
                                     timeMs:Long,
                                     val maxMem:Long,
                                     val slaveEndpoint:ActorRef
                                       ) extends Logging{
  private var _lastSeenMs : Long = timeMs
  private var _remainingMem : Long = maxMem

  private val _blocks = new mutable.HashMap[BlockId,BlockStatus]()

  def updateLastSennMs(): Unit ={
    _lastSeenMs = System.currentTimeMillis()
  }

  def updateBlockInfo(
                     blockId: BlockId,
                     storageLevel: StorageLevel,
                     memSize:Long,
                     diskSize:Long
                       ): Unit ={
    updateLastSennMs()

    /**已经存在的block*/
    if(_blocks.contains(blockId)){
      //TODO
    }

    /**如果不是valid的话，memSize和diskSize都为0，说明block被移除了*/

    var blockStatus : BlockStatus = null
    if(storageLevel.isValid){
      if(storageLevel.useMemory){
        blockStatus = BlockStatus(storageLevel,memSize,0)
        _blocks.put(blockId,blockStatus)
        _remainingMem -= memSize
        logInfo("Added %s in memory on %s (size %s , free :%s)".format(
        blockId,blockManagerId.hostPort)

        )
      }
    }
  }
}