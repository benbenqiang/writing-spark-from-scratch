package io

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer

/**
 * NIO 属于同步非阻塞，使用selector进行轮训
 * 同步阻塞：传统JAVA stream io
 * 同步非阻塞：io一定会返回，不会阻塞，但是不一定会取到结果。需要不断的去取结果
 * 异步阻塞：NIO，在selector上调用select是会阻塞的，结果通过事件驱动的方式返回
 * 异步非阻塞：发起io操作不阻塞，通过回调函数获取数据触发相应操作
 * Created by bbq on 2016/1/21
 */
object NIOByteBufferDemo {
  def fileChannelByteBuffer() = {
    val fileSep = File.separator
    val file = new File(s"scala-learning${fileSep}src${fileSep}main${fileSep}resources${fileSep}FileChanelFile.data")
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
