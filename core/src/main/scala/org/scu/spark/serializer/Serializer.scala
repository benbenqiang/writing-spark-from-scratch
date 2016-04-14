package org.scu.spark.serializer

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

/**
 * Created by bbq on 2016/4/12
 */
abstract class Serializer {
  /**Deserialization 的ClassLoader*/
  protected var defaultClassLoader : Option[ClassLoader] = None

  def setDefaultClassLoader(classLoader: ClassLoader) : Serializer = {
    defaultClassLoader = Some(classLoader)
    this
  }

  def newInstance():SerializerInstance

}

abstract class SerializerInstance{
  def serialize[T:ClassTag](t:T):ByteBuffer

  def deserialize[T:ClassTag](bytes:ByteBuffer):T

  def deserialize[T:ClassTag](bytes:ByteBuffer,loader:ClassLoader):T

  def serializeStream(s:OutputStream):SerializationStream

  def deserializeStream(s:InputStream):DeserializationStream
}

/**
 * 效果与 new ObjectOutputStream.writeObject 相同，
 * 只是可以对写入的前后座更多的定制化，包括 wrtteKey writeValue
 * 同时支持对迭代器的序列化
 *
 * */
abstract class SerializationStream{
  def writeObject[T:ClassTag](t:T):SerializationStream

  /**writeKey 和 writeValue实在unsafe中使用的*/
  def writeKey[T:ClassTag](key:T):SerializationStream = writeObject(key)

  def writeValue[T:ClassTag](value:T):SerializationStream = writeObject(value)

  def flush():Unit

  def close():Unit

  def writeAll[T:ClassTag](iter:Iterator[T]):SerializationStream={
    while(iter.hasNext){
      writeObject(iter.next())
    }
    this
  }
}

abstract class DeserializationStream{
  def readObject[T:ClassTag]():T

  def readKey[T:ClassTag]():T = readObject[T]()

  def readValue[T:ClassTag]():T = readObject[T]()

  def close():Unit


}