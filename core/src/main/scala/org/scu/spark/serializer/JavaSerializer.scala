package org.scu.spark.serializer

import java.io._
import java.nio.ByteBuffer

import org.scu.spark.SparkConf
import org.scu.spark.util.ByteBufferOutputStream

import scala.reflect.ClassTag

/**
 * Created by bbq on 2016/4/13
 */
private[spark] class JavaSerializerInstance(
                                           counterRest:Int,
                                           extraDebugInfo:Boolean,
                                           defaultClassLoader:ClassLoader
                                             ) extends SerializerInstance {

  /**将对象用java序列化方式转化为ByteBuffer*/
  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    bos.toByteBuffer
  }

  /**生成序列化流，先将内容写入stream中，然后转化成nio 中的ByteBuffer*/
  override def serializeStream(s: OutputStream): SerializationStream = ???

  override def deserializeStream(s: InputStream): DeserializationStream = ???

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = ???

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = ???
}



class JavaSerializer (conf:SparkConf) extends Serializer with Externalizable{
  private var counterReset = conf.getInt("spark.serializer.objectStreamRest",100)
  private var extraDebugInfo = conf.getBoolean("spark.serializer.extraDebugInfo",true)

  /**仅仅在反序列化的时候使用*/
  protected def this() = this(new SparkConf())

  override def newInstance(): SerializerInstance = {
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread().getContextClassLoader)
    new JavaSerializerInstance(counterReset,extraDebugInfo,classLoader)
  }

  override def readExternal(in: ObjectInput): Unit = {
    counterReset = in.readInt()
    extraDebugInfo = in.readBoolean()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(counterReset)
    out.writeBoolean(extraDebugInfo)
  }
}
