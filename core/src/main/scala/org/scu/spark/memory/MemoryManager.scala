package org.scu.spark.memory

import org.scu.spark.{Logging, SparkConf}

/**
 * 每一个Executor的内存都会分为执行内存和存储内存（execution and storage）
 * Created by bbq on 2016/4/26
 */
private[spark] abstract class MemoryManager(
                                           conf:SparkConf,
                                           numCores:Int,
                                           onHeapStorageMemory:Long,
                                           onHeapExecutionMemory:Long
                                             ) extends Logging{

}
