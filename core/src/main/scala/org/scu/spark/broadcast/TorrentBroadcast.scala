package org.scu.spark.broadcast

import org.scu.spark.Logging

import scala.reflect.ClassTag

/**
 * Created by bbq on 2016/5/5
 */
private[spark] class TorrentBroadcast[T:ClassTag](obj:T,id:Long) extends Broadcast[T](id) with Logging with Serializable{

}


private object TorrentBroadcast extends Logging{
  def unperist(id:Long,removeFromDriver:Boolean,blocking:Boolean):Unit ={
    logDebug("Unpersisting TorrentBroadcast $id")
    //TODO using blockManager to Remove
  }
}