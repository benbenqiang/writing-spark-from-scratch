package org.scu.spark.storage.memory


import java.util

import org.scu.spark.{Logging, SparkConf}
import org.scu.spark.storage.{BlockId, BlockInfoManager}
import org.scu.spark.storage.memory.MemoryMode.MemoryMode
import org.scu.spark.util.io.ChunkedByteBuffer

import scala.reflect.ClassTag

/**
 * Created by bbq on 2016/5/12
 */
private sealed trait MemoryEntry[T]{
  def size:Long
  def memoryMode:MemoryMode
  def classTag : ClassTag[T]
}

/**OFF-Heap 必须序列化*/

private case class DeserializedMemoryEntry[T](
                                             value:Array[T],
                                             size:Long,
                                             classTag:ClassTag[T])  extends MemoryEntry[T]{
  val memoryMode = MemoryMode.ON_HEAP
}

/**序列化内存，将java对象序列化后用ChunkedByteBuffer存储*/
private case class SerializedMemoryEntry[T](
                                           buffer:ChunkedByteBuffer,
                                           memoryMode:MemoryMode,
                                           classTag:ClassTag[T]
                                             )extends  MemoryEntry[T]{
  def size:Long = buffer.size
}



/**
 * 暂时不考虑内存放不下的情况
 * */
private[spark] class MemoryStore(
                                conf:SparkConf,
                                blockInfoManager:BlockInfoManager
                                //TODO serializerManager
                                //TODO MemroyManager
                                //TODO blockEvictionHandler
                                  ) extends Logging{

  /**！！对于非序列化的on-heap都是存储在entires中！！*/
  private val entries = new util.LinkedHashMap[BlockId,MemoryEntry[_]](32,0.75f,true)

  /**先不考虑内存放不下的情况*/
  private[storage] def putIteratorAsValue[T](
                                            blockId:BlockId,
                                            values:Iterator[T],
                                            classTag:ClassTag[T]
                                              ): Long = {
    require(!contains(blockId),s"Block $BlockId is already present in the MemoryStore")

    implicit val tag = classTag
    val arrayValue = values.toArray
    /**估计大小，直接存入*/
    //TODO 估计大小
    val entry = new DeserializedMemoryEntry[T](arrayValue,1000,classTag)
    entry.size
  }

  def contains(blockId: BlockId) : Boolean = {
    entries.synchronized { entries.containsKey(blockId)}
  }
  
  def getSize(blockId:BlockId):Long={
    entries.synchronized{
      entries.get(blockId).size
    }
  }
} 

/**当MemoryStorre.putIteratorAsValues的时候返回的*/

private[storage] class PartiallyUnrolledIterator[T](
                                                   memoryStroe:MemoryStore,
                                                   unrollMamory:Long,
                                                   unrolled:Iterator[T],
                                                   rest:Iterator[T]
                                                     ) extends Iterator[T]{


  override def hasNext: Boolean = ???

  override def next(): T = ???
}