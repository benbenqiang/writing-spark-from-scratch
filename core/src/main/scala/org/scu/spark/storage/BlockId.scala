package org.scu.spark.storage

/**
 * 每一blockId对应多个逻辑上的存储快/数据。
 * Created by bbq on 2016/5/8
 */
sealed abstract class BlockId {

  def name:String

  def isRDD : Boolean = isInstanceOf[RDDBlockId]
  def isShuffle:Boolean = isInstanceOf[ShuffleBlockId]
  def isBroadcast:Boolean = isInstanceOf[BroadcastBlockId]

  override def hashCode(): Int = name.hashCode
  override def equals(other: scala.Any): Boolean = other match {
    case o:BlockId => this.getClass == o.getClass && name.equals(o.name)
    case _ => false
  }
  override def toString: String = name
}


case class RDDBlockId(rddId:Int,splitIndex:Int) extends BlockId{
  override def name: String = "rdd_" + rddId + "_" + splitIndex
}

/**每次shuffle中的每个map多输出的不同reduce数据，有不同的blockId，分发到不同的reduce节点。（基于hashshuffle，会生成很多文件）*/
case class ShuffleBlockId(shuffleId:Int,mapId:Int,reduceId:Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
}

/**下面两个index 和data 是基于sort的hash，每个map只有两个输出文件，一个是index 一个是data文件*/
case class ShuffleIndexBlockId(shuffleId:Int,mapId:Int,reduceId:Int) extends BlockId{
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".index"
}

case class ShuffleDataBlockId(shuffleId:Int,mapId:Int,reduceId:Int) extends BlockId{
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data"
}

case class BroadcastBlockId(broadcastId:Long,field:String="") extends BlockId{
  override def name: String = "broadcast_" + broadcastId + (if(field == "") "" else "_" + field)
}

case class TaskResultBlockId(taskId:Long) extends BlockId {
  override def name: String = "taskresult_" + taskId
}

case class StreamBlockId(streamId:Int,uniqueId:Long) extends BlockId{
  override def name: String = "input-"+streamId+"-"+uniqueId
}

object BlockId{
  val RDD = "rdd_([0-9]+)_([0-9]+)".r
  val SHUFFLE = "shuffle_([0-9]+)_([0-9]+)_([0-9]_+)".r
  val SHUFFLE_DATA = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).data".r
  val SHUFFLE_INDEX = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).index".r
  val BROADCAST = "broadcast_([0-9]+)_([_A-Za-z0-9]*)".r
  val TASKRESULT = "taskresutlt_([0-9]+)".r
  val STREAM = "input-([0-9]+)-([0-9]+)".r

  def apply(id:String):BlockId = id match {
    case RDD(rddId, splitIndex) =>
      RDDBlockId(rddId.toInt, splitIndex.toInt)
    case SHUFFLE(shuffleId, mapId, reduceId) =>
      ShuffleBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case SHUFFLE_DATA(shuffleId, mapId, reduceId) =>
      ShuffleDataBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case SHUFFLE_INDEX(shuffleId, mapId, reduceId) =>
      ShuffleIndexBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case BROADCAST(broadcastId,field) =>
      BroadcastBlockId(broadcastId.toInt,field)
    case TASKRESULT(taskId)=>
      TaskResultBlockId(taskId.toLong)
    case STREAM(streamId,uniqueId)=>
      StreamBlockId(streamId.toInt,uniqueId.toInt)
    case _=>
      throw new IllegalStateException("Unrecognized BlockId:"+ id)
  }
}
