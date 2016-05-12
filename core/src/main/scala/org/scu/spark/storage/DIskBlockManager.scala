package org.scu.spark.storage

import org.scu.spark.{Logging, SparkConf}

/**
 * 创建和维护block对应的物理存储位置关系表。一个block对应到一个file
 * Created by bbq on 2016/5/12
 */
private[spark] class DiskBlockManager(conf:SparkConf,deleteFileOnStop:Boolean) extends Logging{

}
