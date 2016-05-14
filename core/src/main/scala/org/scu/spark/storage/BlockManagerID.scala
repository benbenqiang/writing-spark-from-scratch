package org.scu.spark.storage

import java.io.{ObjectInput, ObjectOutput, Externalizable}
import java.util.concurrent.ConcurrentHashMap

import org.scu.spark.SparkContext

/**
 * 唯一代表一个BlockManager
 * Created by bbq on 2015/11/26
 */
class BlockManagerId private (
                      private var executorID_ : String,
                      private var host_ : String,
                      private var port_ : Int
                      ) extends Externalizable{
  def executorId :String = executorID_

  def isDriver : Boolean = {
    executorId == SparkContext.DRIVER_IDENTIFIER
  }

  def hostPort : String = {
    assert( port > 0 )
    host + ":" + port
  }

  def port :Int = port_

  def host : String = host_

  override def readExternal(in: ObjectInput): Unit = {

  }

  override def writeExternal(out: ObjectOutput): Unit = {

  }
}
private [spark] object BlockManagerId{

  def apply(execId:String,host:String,port:Int):BlockManagerId = getCachedBlockManagerId(new BlockManagerId(execId,host,port))

  val blockManagerIdCache = new ConcurrentHashMap[BlockManagerId,BlockManagerId]()

  def getCachedBlockManagerId(id:BlockManagerId):BlockManagerId ={
    blockManagerIdCache.putIfAbsent(id,id)
    blockManagerIdCache.get(id)
  }
}

