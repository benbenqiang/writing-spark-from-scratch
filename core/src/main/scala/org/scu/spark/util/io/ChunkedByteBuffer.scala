package org.scu.spark.util.io

import java.nio.ByteBuffer

/**
 * blockId 的数据会被分成许多个小的chunks，然后存储到内存或者磁盘中
 * Created by bbq on 2016/5/12
 */
private[spark] class ChunkedByteBuffer(var chunks:Array[ByteBuffer]) {
  /**保证传入的数据不为空，保证每个ByteBuffer不为空，保证ByteBuffer的position都在0*/
  require(chunks != null,"chunks must not be null")
  require(chunks.forall(x=>x.limit() > 0 && x.position() == 0 ),"chunks must non-empty and position must be 0")

  private[this] var disposed : Boolean = false

  val size:Long = chunks.map(_.limit()).sum

}
