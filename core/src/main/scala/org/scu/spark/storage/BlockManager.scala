package org.scu.spark.storage

import java.util.concurrent.ConcurrentHashMap

import org.scu.spark.storage.memory.{MemoryStore, PartiallyUnrolledIterator}
import org.scu.spark.{Logging, SparkConf}
import org.scu.spark.rpc.akka.AkkaRpcEnv

import scala.reflect.ClassTag

/**
 * Driver 和Executor 端都有这个类，负责对block的存储和读取（远程或者本地）。
 * Created by bbq on 2016/5/9
 */
private[spark] class BlockManager(
                                 executorId:String,
                                 rpcEnv:AkkaRpcEnv,
                                 val master : BlockManagerMaster,
                                 val conf : SparkConf,

                                 numUsableCores:Int
                                   )  extends Logging{

  private[storage] val blockInfoManager = new BlockInfoManager

  /**每个blockId对应一个Blcokinfo*/
  private[storage] val blockInfo = new ConcurrentHashMap[BlockId,BlockInfo]

  private[spark] val memoryStore = new MemoryStore(conf,blockInfoManager)
  /**管理Blockmanager存在硬盘的数据*/
  val diskBlockManager = new DiskBlockManager(conf,false)
  private[spark] val diskStore = new DiskStore(conf,diskBlockManager)

  /**存储一个对象到BlockManager中
    * @return 成功返回true，失败或者已经存储过返回false
    * */
  def putSingle[T : ClassTag](
                             blockId:BlockId,
                             value:T,
                             level:StorageLevel,
                             tellMaster:Boolean=true
                               ) : Boolean = {
    putIterator(blockId,Iterator(value),level,tellMaster)
  }

  def putIterator[T:ClassTag](
                               blockId:BlockId,
                               values:Iterator[T],
                               level:StorageLevel,
                               tellMaster:Boolean=true
                               ) : Boolean = {
    require(values != null,"Values is null")
    doPutIterator(blockId,()=>values,level,implicitly[ClassTag[T]],tellMaster) match{
      case None => true
      case Some(iter) => false
    }
  }

  /**
   *  将BlockId对应的数据存储到内存或者磁盘中
    * @param classTag 存储数据类型的类信息，因为jvm在编译的时候会进行类型擦出，所以需要保存
    * @return None 说明存放成功
    */
  private def doPutIterator[T](
                                blockId:BlockId,
                                iterator:() => Iterator[T],
                                level:StorageLevel,
                                classTag:ClassTag[T],
                                tellMaster:Boolean = true,
                                keepReadLock:Boolean = false
                                ): Option[PartiallyUnrolledIterator[T]] = {
    doPut(blockId,level,classTag,tellMaster,keepReadLock){ info =>{
      val startTime = System.currentTimeMillis()

      var size = 0L

      if(level.useMemory){
        //TODO 暂时不考虑当内存放不下了
        /**将block的数据放在内存中*/
        size = memoryStore.putIteratorAsValue(blockId,iterator(),classTag)
      }else{
        //TODO 将数据存入磁盘
        logError("还没有实现将数据存入磁盘，只支持内存")
      }
      None
    }}
  }

  private def doPut[T](
                      blockId:BlockId,
                      level:StorageLevel,
                      classTag:ClassTag[_],
                      tellMaster:Boolean,
                      keepReadLock:Boolean
                        )(putBody:BlockInfo => Option[T]):Option[T]={
    require(blockId != null ,"BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")

    /**生成该BlockId的Blockinfo，如果之前这个BlockID已经存储过，那么直接返回false*/
    val putBlockInfo ={
      val newInfo = new BlockInfo(level,classTag,tellMaster)
      //TODO read and write lock by blockInfoManager
      val oldInfo = Option(blockInfo.putIfAbsent(blockId,newInfo))
      if(oldInfo.isDefined){
        logWarning(s"Block $blockId already exists on this machine;not re-adding it")
        return None
      }else{
        newInfo
      }
    }
    /**存储BlockId*/
    val result = putBody(putBlockInfo)
    result
  }
}
