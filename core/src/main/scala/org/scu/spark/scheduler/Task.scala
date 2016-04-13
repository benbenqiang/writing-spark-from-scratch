package org.scu.spark.scheduler

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import org.scu.spark.Accumulator
import org.scu.spark.serializer.SerializerInstance
import org.scu.spark.util.{ByteBufferInputStream, Utils, ByteBufferOutputStream}

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

  /**把Task以及对应以来的jar包以及文件进行序列化
    * 转换成ByteBuffer，放到TaskDescription中
    * ByteBuffer本身是没有实现序列化的，在TaskDescription中通过自定义readObject和wrtieObject
    * */
  def serializeWithDependencies(
                               task:Task[_],
                               currentFiles:mutable.HashMap[String,Long],
                               currentJars:mutable.HashMap[String,Long],
                               serializer:SerializerInstance
                                 ):ByteBuffer={
    val out = new ByteBufferOutputStream(4096)
    val dataOut = new DataOutputStream(out)

    dataOut.writeInt(currentFiles.size)
    for((name,timestamp) <- currentFiles){
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    dataOut.writeInt(currentJars.size)
    for((name,timestamp) <- currentJars){
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    dataOut.flush()
    val taskBytes = serializer.serialize(task)
    Utils.writeByteBuffer(taskBytes,out)
    out.toByteBuffer
  }

  /**
   * 从ByteBuffer中将依赖抽取出来
   * 用stream的方式从ByteBuffer中读取数据
   * */
  def deserializeWithDependencies(serializedTask:ByteBuffer)
  :(mutable.HashMap[String,Long],mutable.HashMap[String,Long],ByteBuffer)={
    val in = new ByteBufferInputStream(serializedTask)
    val dataIn = new DataInputStream(in)

    val taskFiles = new mutable.HashMap[String,Long]()
    val numFiles = dataIn.readInt()
    for(i <- 0 until numFiles){
      taskFiles(dataIn.readUTF()) = dataIn.readLong()
    }

    val taskJars = new mutable.HashMap[String,Long]()
    val numJars = dataIn.readInt()
    for(i <- 0 until numJars){
      taskJars(dataIn.readUTF())=dataIn.readLong()
    }

    val subBuffer = serializedTask.slice()
    (taskFiles,taskJars,subBuffer)
  }

}
