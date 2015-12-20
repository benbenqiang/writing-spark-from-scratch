package org.scu.spark.deploy.master

/**
 * Created by bbq on 2015/12/20
 */
private[master] class ExecutorDesc (
                                   val id : Int,
                                   val application:ApplicationInfo,
                                   val worker:WorkerInfo,
                                   val cores:Int,
                                   val memory:Int
                                     ){

}
