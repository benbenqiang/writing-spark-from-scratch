package org.scu.spark.scheduler

import org.scu.spark.rdd.RDD

/**
 * 作为DAG的中间stage，有shuffle
 * Created by bbq on 2015/11/22
 */
class ShuffleMapStage(
                     id:Int,
                     rdd:RDD[_],
                     numTasks:Int,
                     parents:List[Stage],
                     firstJobId:Int
                       ){

}
