package org.scu.spark.util

import java.io.InputStream
import java.nio.ByteBuffer

/**
 * 从ByteArray中以stream的方式读取数据
 * Created by bbq on 2016/4/13
 */
class ByteBufferInputStream(private var buffer: ByteBuffer, dispose: Boolean = false) extends InputStream {
  /**获取一个字节*/
  override def read(): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp()
      -1
    }else{
      buffer.get() & 0xFF
    }
  }

  override def read(dest:Array[Byte]):Int={
    read(dest,0,dest.length)
  }

  override def read(dest:Array[Byte],offset:Int,length:Int):Int={
    if(buffer==null || buffer.remaining() == 0){
      cleanUp()
      -1
    } else {
      val ammountToGet = math.min(buffer.remaining(),length)
      buffer.get(dest,offset,ammountToGet)
      ammountToGet
    }
  }

  override def skip(bytes:Long):Long={
    if(buffer != null){
      val amountToSkip = math.min(bytes,buffer.remaining()).toInt
      buffer.position(buffer.position()+ amountToSkip)
      if(buffer.remaining() == 0){
        cleanUp()
      }
      amountToSkip
    }else{
      0L
    }
  }

  /**对于memory-mapped file，需要cleanup*/
  private def cleanUp() = {
    ???
  }
}
