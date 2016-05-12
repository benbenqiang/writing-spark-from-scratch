package org.scu.spark.storage

import org.scu.spark.{Logging, SparkConf}

/**
 * 将BlockManager存储到Disk上
 * Created by bbq on 2016/5/12
 */
private[spark] class DiskStore(conf:SparkConf,diskManager:DiskBlockManager)extends Logging{

  private val minMemoryMapBytes = conf.getInt("spark.storage.memoryMapThreshold",2*1024*1024)



}
