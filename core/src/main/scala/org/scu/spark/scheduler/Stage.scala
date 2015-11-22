package org.scu.spark.scheduler

import org.scu.spark.Logging
import org.scu.spark.rdd.RDD

/**
 * Stage定义：包含一组功能相同的Task集合，task可以并行执行，包含于job中，所有的task拥有相同的shuffle以来。
 * DAG的划分就是根据shuffle边界。
 *
 * Stage有两种：shuffleMapStage和ResultStage.
 * 每个stage会记录第一次提交的jobID，统计记录所有的jobID，用于FIFO调度
 * Created by bbq on 2015/11/22
 */
private[scheduler] abstract class Stage(
                                       val id:Int,
                                       val rdd:RDD[_],
                                       val numTask:Int,
                                       val paraents:List[Stage],
                                       val firstJobId:Int
                                         ) extends Logging{

}
