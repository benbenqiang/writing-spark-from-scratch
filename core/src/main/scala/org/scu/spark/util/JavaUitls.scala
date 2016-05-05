package org.scu.spark.util

import java.nio.ByteBuffer

/**
 * 在源码中，这个是在另外一个模块的Java类
 * Created by bbq on 2016/5/5.
 */
object JavaUitls {

  /**将buffer转换为Array[Byte]*/
  def bufferToArray(buffer:ByteBuffer):Array[Byte]={
    if(buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.array().length == buffer.remaining()){
      buffer.array()
    }else{
      val bytes = new Array[Byte](buffer.remaining())
      buffer.get(bytes)
      bytes
    }
  }
}
