package org.scu.spark.storage.memory

/**
 * Created by bbq on 2016/5/12
 */
object MemoryMode extends Enumeration{
  val ON_HEAP,OFF_HEAP = Value
  type MemoryMode = Value
}
