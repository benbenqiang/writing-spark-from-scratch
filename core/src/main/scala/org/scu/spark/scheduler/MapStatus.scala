package org.scu.spark.scheduler

import org.scu.spark.storage.BlockManagerID

/**
 * shuffleMap任务的运行状态反馈：包括BlockManagerID和map的输出数据
 * Created by bbq on 2015/11/26
 */
private[spark] sealed trait MapStatus {
  /**当前任务运行的BlockManager*/
  def location :BlockManagerID

  /**
   * 估计reduce任务的大小，如果为空那么reduce就不会来fetch
   */
  def getSizeForBlock(reduceId:Int):Long
}
