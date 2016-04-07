package org.scu.spark.scheduler.cluster

import java.nio.ByteBuffer

import org.scu.spark.util.SerializableBuffer

/**
 * Created by bbq on 2016/4/7
 */
private[spark] class TaskDescription(
                                    val taskId :Long,
                                    val attemptNumber:Int,
                                    val executorId:String,
                                    val name:String,
                                    val index:Int,
                                    /**没有被def使用，所以不会被升格为private val,这个字段不会被序列化*/
                                    _serializedTask:ByteBuffer
                                      ) extends Serializable{
  /**ByteBuffer不支持序列化，因此通过包装一个类定制序列化*/
  private val buffer = new SerializableBuffer(_serializedTask)

  def serializedTask : ByteBuffer = buffer.value

  override def toString: String = s"TaskDescription(TID= $taskId, index= $index)"
}
