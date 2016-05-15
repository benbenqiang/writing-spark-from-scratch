package org.scu.spark.storage

import akka.actor.{Actor, ActorRef}
import org.scu.spark.storage.BlockManagerMessage.UpdateBlockInfo
import org.scu.spark.{Logging, SparkConf}
import org.scu.spark.rpc.akka.AkkaRpcEnv
import org.scu.spark.util.Utils

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

  private val blockLocations = new mutable.HashMap[BlockId,mutable.HashSet[BlockManagerId]]

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

    /**更新blockManagerId -> blockManagerInfo 信息*/
    blockManagerInfo(blockManagerId).updateBlockInfo(blockId,storageLevel,memSize,diskSize)

    /**更新blockId -> BlockManagerID*/
    var locations : mutable.HashSet[BlockManagerId] = null


    if(blockLocations.contains(blockId)){
      locations = blockLocations.get(blockId).get
    }
    else{
      val locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId,locations)
    }

    if(storageLevel.isValid) {
      locations.remove(blockManagerId)
    }else{
      locations.add(blockManagerId)
    }

    if(locations.isEmpty){
      blockLocations.remove(blockId)
    }
    true
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

  /**包含该blockManager不包含broadcast的block*/
  private val _cachedBlocks = new mutable.HashSet[BlockId]

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
      /**新添加的block的内存和硬盘空间*/
      if(storageLevel.useMemory){
        blockStatus = BlockStatus(storageLevel,memSize,diskSize=0)
        _blocks.put(blockId,blockStatus)
        _remainingMem -= memSize
        logInfo("Added %s in memory on %s (size: %s , free: %s)".format(
        blockId,blockManagerId.hostPort,Utils.bytesToString(memSize),Utils.bytesToString(_remainingMem)))
      }
      if(storageLevel.useDisk){
        blockStatus = BlockStatus(storageLevel,memSize=0,diskSize)
        _blocks.put(blockId,blockStatus)
        logInfo("Added %s on disk on %s (size: %s)".format(blockId,blockManagerId.hostPort,Utils.bytesToString(diskSize)))
      }
      /**不记录广播变量,并且内存中和磁盘中的大小都不为零*/
      if(!blockId.isBroadcast && blockStatus.isCached){
        _cachedBlocks += blockId
      }
    }else if (_blocks.contains(blockId)){
      val blockStatus:BlockStatus = _blocks.get(blockId).get
      _blocks.remove(blockId)
      _cachedBlocks -= blockId
      if(blockStatus.storageLevel.useMemory){
        logInfo("Removed %s on %s in memory (size: %s,free: %s)".format(
          blockId,blockManagerId.hostPort,Utils.bytesToString(blockStatus.memSize),Utils.bytesToString(_remainingMem)))
      }
      if(blockStatus.storageLevel.useDisk){
        logInfo("Removed %s on %s in disk (size: %s)".format(
          blockId,blockManagerId.hostPort,Utils.bytesToString(blockStatus.diskSize)))
      }
    }
  }
}