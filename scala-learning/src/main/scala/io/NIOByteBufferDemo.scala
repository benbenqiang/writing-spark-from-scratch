package io

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer

/**
 * NIO 属于同步非阻塞，使用selector进行轮训
 * 同步阻塞：传统JAVA stream io
 * 同步非阻塞：NIO，io操作不阻塞，但是获取io数据需要查询，因此是同步
 * 异步阻塞：io操作还是会阻塞一个线程，但是可以通过futhure的方式获取数据
 * 异步非阻塞：发起io操作，通过回掉获取数据
 * Created by bbq on 2016/1/21
 */
object NIOByteBufferDemo {
  def fileChannelByteBuffer() = {
    val fileSep = File.separator
    val file = new File(s"scala-learning${fileSep}src${fileSep}resources${fileSep}FileChanelFile.data")
    val stream = new FileOutputStream(file)
    val channel = stream.getChannel
    val buffer = ByteBuffer.allocate(1024)
    val string = "writing something"
    buffer.put(string.getBytes)
    buffer.flip()
    channel.write(buffer)
    channel.close()
  }

  def main(args: Array[String]) {
    fileChannelByteBuffer()
  }
}
