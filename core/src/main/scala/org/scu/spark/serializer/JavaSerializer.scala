package org.scu.spark.serializer

import java.io._
import java.nio.ByteBuffer

import org.scu.spark.SparkConf
import org.scu.spark.util.{ByteBufferInputStream, ByteBufferOutputStream}

import scala.reflect.ClassTag

/**
 * Created by bbq on 2016/4/13
 */

/**对java对象进行序列化
  * 其实就是对我们常用的ObjectOutputStream进行了包装，在writeObject的时候，为了防止内存溢出，使用了reset方法
  * */
private[spark] class JavaSerializationStream(
                                            out:OutputStream,counterRest:Int,extraDebugInfo:Boolean
                                              ) extends SerializationStream{
  private val objOut = new ObjectOutputStream(out)
  private var counter = 0

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    try{
      objOut.writeObject(t)
    }catch {
      case e : NotSerializableException if extraDebugInfo =>
        //TODO SerializationDebugger
        throw e
    }
    counter += 1
    if(counterRest > 0 && counter >= counterRest){
      objOut.reset()
      counter = 0
    }
    this
  }

  override def flush(): Unit = {objOut.flush()}

  override def close(): Unit = {objOut.close()}
}

private[spark] class JavaDeserializationStream(
                                                in:InputStream,
                                                loader: ClassLoader
                                                ) extends DeserializationStream{

  /** 为了制定classLoader，这里就不做了
    * */
  private val objIn = new ObjectInputStream{
    //TODO customize_classLoader
  }

  override def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]

  override def close(): Unit = objIn.close()
}


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
  override def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s,counterRest,extraDebugInfo)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s,defaultClassLoader)
  }

   def deserializeStream(s:InputStream,loader:ClassLoader) : DeserializationStream={
    new JavaDeserializationStream(s,loader)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject()
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis,loader)
    in.readObject()
  }
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
