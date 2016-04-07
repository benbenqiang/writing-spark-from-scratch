package io.serialize

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.Channels

/**
 * javaNIO 中的byteBuffer不支持序列化，我们需要通过readObject 和 writeObject 定制化ByteBuffer的序列化
 * Created by bbq on 2016/4/7
 */
object SerializeCustom {
  def main(args: Array[String]) {
    /**分配一块内存空间*/
    val buffer = ByteBuffer.allocate(100)
    buffer.put("hehesssssssssssssssssssssda".getBytes())
    buffer.flip()
    /**读写的文件*/
    val fileSep = File.separator
    val file = new File(s"scala-learning${fileSep}src${fileSep}main${fileSep}resources${fileSep}SerializeCustom.data")
    val out = new ObjectOutputStream(new FileOutputStream(file))
    out.writeInt(1)
    out.writeObject(new SerializeCustom(buffer))
    out.close()
    println("写入成功")

    /**从文件中读取文件*/
    val in = new ObjectInputStream(new FileInputStream(file))
    println("read int : "+in.readInt())
    val serilized: SerializeCustom = in.readObject().asInstanceOf[SerializeCustom]
    val byteArray = new Array[Byte](serilized.buffer.limit())
    serilized.value.get(byteArray)
    println(new String(byteArray))
    in.close()
  }
}

class SerializeCustom(@transient var buffer :ByteBuffer) extends Serializable{
  def value : ByteBuffer = buffer

  /**这里必须声明unit 不然序列化通过反射找不到*/
  private def readObject(in:ObjectInputStream):Unit={
    val length = in.readInt()
    buffer = ByteBuffer.allocate(length)
    var amountRead = 0
    val channel = Channels.newChannel(in)
    while(amountRead < length){
      val ret = channel.read(buffer)
      if(ret == -1)
        throw new EOFException("End of file before fully reading buffer")
      amountRead += ret
    }
    buffer.rewind()
  }

  private def writeObject(out:ObjectOutputStream):Unit={
    out.writeInt(buffer.limit())
    if(Channels.newChannel(out).write(buffer) != buffer.limit()){
      throw new IOException("Could not fully write buffer to output stream")
    }
    buffer.rewind()
  }

}