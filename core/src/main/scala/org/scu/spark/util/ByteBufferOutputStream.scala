package org.scu.spark.util

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

/**
 * 将ByteArrayOutputStream包装成ByteBuffer
 * 这里是zero-copy 对buffer的修改会影响bytreArray的内容，反之同理。
 * Created by bbq on 2016/4/13
 */
private[spark] class ByteBufferOutputStream(capacity: Int) extends ByteArrayOutputStream {
  def this() = this(32)

  def getCount(): Int = count

  def toByteBuffer : ByteBuffer = {
    ByteBuffer.wrap(buf,0,count)
  }

}
