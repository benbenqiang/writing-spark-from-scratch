package org.scu.spark.broadcast

import org.scu.spark.rdd.SparkException
import org.scu.spark.storage.{StorageLevel, BroadcastBlockId}
import org.scu.spark.{SparkEnv, SparkConf, Logging}

import scala.reflect.ClassTag

/**
 * 按照比特流的的方式实现。运行机制：driver端将需要序列化的对象进行分片，然后将分片存储到blockManager中。
 * 每个executor从本地blockManager获取数据，如果数据不存在，那么就通过远程来去的方式以分片为单位从driver或者其他节点获取。
 * 当获取到一个分片的时候，将分片放入自身的blockManager，可以提供给其他executors。（这种方式避免了driver成为性能瓶颈。）
 * Created by bbq on 2016/5/5
 */
private[spark] class TorrentBroadcast[T:ClassTag](obj:T,id:Long) extends Broadcast[T](id) with Logging with Serializable{

  private var blockSize : Int = _

  private def setConf(conf:SparkConf): Unit = {
    //TODO code compress
    /**每个数据块的大小，单位是Byte*/
    blockSize = conf.get("spark.broadcast.blockSize","4").toInt * 1024 * 1024
  }

  setConf(SparkEnv.env.conf)

  private val broadcasId = BroadcastBlockId(id)

  private val numBlocks : Int = writeBlocks(obj)

  private def writeBlocks(value:T):Int={
    import StorageLevel._
    val blockManager = SparkEnv.env.blockManager
    /**当前只存一份*/
    if(!blockManager.putSingle(broadcasId,value,MEMORY_ONLY,tellMaster = true)){
      throw new SparkException(s"Failed to stroe $broadcasId in BlockManager")
    }
    logDebug("Successfully put value into broadcast")

    /**返回数据被切割成block的个数，因为这里目前不考虑数据分片，直接将数据存入内存并通知blockManagerMaster*/
    1
  }

}


private object TorrentBroadcast extends Logging{
  def unperist(id:Long,removeFromDriver:Boolean,blocking:Boolean):Unit ={
    logDebug("Unpersisting TorrentBroadcast $id")
    //TODO using blockManager to Remove
  }
}