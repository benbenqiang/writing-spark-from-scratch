package org.scu.spark.util

import java.io.{IOException, ObjectOutputStream, EOFException, ObjectInputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels

/**
 * Created by bbq on 2016/4/7
 */
private[spark] class SerializableBuffer(@transient var buffer :ByteBuffer) extends Serializable{
  def value :ByteBuffer = buffer

  private def readObject(in:ObjectInputStream)={
    val lenght = in.readInt()
    buffer = ByteBuffer.allocate(lenght)
    var amountRead = 0
    val channel = Channels.newChannel(in)
    while(amountRead < lenght){
      val ret = channel.read(buffer)
      if(ret == -1){
        throw new EOFException("End of file before fully reading buffer")
      }
      amountRead += ret
    }
    buffer.rewind()
  }

  private def writeObject(out:ObjectOutputStream)={
    out.writeInt(buffer.limit())
    if(Channels.newChannel(out).write(buffer) != buffer.limit()){
      throw new IOException("Could not fully write buffer to output stream")
    }
    buffer.rewind()
  }
}
