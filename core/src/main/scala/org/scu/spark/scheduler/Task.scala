package org.scu.spark.scheduler

import java.nio.ByteBuffer

import org.scu.spark.Accumulator
import org.scu.spark.serializer.SerializerInstance

import scala.collection.mutable

/**
 * Spark中执行单元,有两种
 *
 * 1.scheduler.ShuffleMapTask
 * 2.scheduler.ResultTask
 *
 * 一个SparkJob分为一个或者多个stage。最后一个包含很多ResultTask，之前的stage由ShuffleMapTask组成。
 * Created by bbq on 2015/12/7
 */
private[spark] abstract class Task[T](
                                     val stageId : Int,
                                     val stageAttemptId:Int,
                                     val partitionId:Int,
                                     internalAccumulators:Seq[Accumulator[Long]]
                                       ) extends Serializable{
}

private[spark] object Task{

  /**把Task以及对应以来的jar包以及文件进行序列化*/
  def SerializeWithDependencies(
                               task:Task[_],
                               currentFiles:mutable.HashMap[String,Long],
                               currrentJars:mutable.HashMap[String,Long],
                               serializer:SerializerInstance
                                 ):ByteBuffer={
    ???
  }
}
