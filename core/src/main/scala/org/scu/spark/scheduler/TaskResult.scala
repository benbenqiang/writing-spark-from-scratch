package org.scu.spark.scheduler

import java.io.{ObjectInput, ObjectOutput, Externalizable}
import java.nio.ByteBuffer

import org.scu.spark.SparkEnv
import org.scu.spark.util.Utils

/**
 * TaskResult : 当Executor运算完成后，将结果放入TaskResult中。
 * 有两个子类，当结果小于阈值的时候，结果放入DirectTaskResult中，大于阈值放入IndirectTaskResult，通过BlockManager返回结果
 * Created by bbq on 2016/5/3
 */
private[spark] sealed trait TaskResult[T]

private[spark] case class IndirectTaskRsult[T](//TODO blcokId
                                               size:Int) extends TaskResult[T] with Serializable
private[spark] class DirectTaskResult[T](
                                          var valueBytes:ByteBuffer
                                        //TODO : accumUpdates
                                          ) extends TaskResult[T] with Externalizable{
  private var valueObjectDeserialized = false
  private var valueObject : T  = _

  /**externalizable 需要无参的构造函数*/
  def this() = this(null.asInstanceOf[ByteBuffer])

  override def readExternal(in: ObjectInput): Unit = {
    val blen = in.readInt()
    val byteVal = new Array[Byte](blen)
    in.readFully(byteVal)
    /**通过wrap方法将ByteBuffer还原*/
    valueBytes = ByteBuffer.wrap(byteVal)
    //TODO read AccumUpdates
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(valueBytes.remaining())
    Utils.writeByteBuffer(valueBytes,out)
    //TODO write AccumUpdates
  }

  /**第一次调用的时候，需要对valueByts进行反序列化，结果存入valueObject中*/
  def value():T={
    if(valueObjectDeserialized){
      valueObject
    }else {
      val resultSer = SparkEnv.env.serializer.newInstance()
      valueObject = resultSer.deserialize(valueBytes)
      valueObjectDeserialized = true
      valueObject
    }
  }
}
