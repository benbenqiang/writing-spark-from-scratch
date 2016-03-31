package org.scu.spark.scheduler.cluster

/**
 * 存储Executor的信息，给SparkListeners使用
 * Created by bbq on 2016/3/31
 */
class ExecutorInfo (
                   val executorHost:String,
                   val totalCores:Int,
                   val logUrlMap:Map[String,String]
                     ){
  def canEqual(other:Any):Boolean = other.isInstanceOf[ExecutorInfo]

  override def equals(other:Any) :Boolean=other match{
    case that : ExecutorInfo=>
      (that canEqual this) &&
      executorHost == that.executorHost &&
      totalCores == that.totalCores &&
      logUrlMap == that.logUrlMap
    case _ => false
  }

  override def hashCode():Int={
    val state = Seq(executorHost,totalCores,logUrlMap)
    state.map(_.hashCode()).foldLeft(0)((a,b)=>31*a + b)
  }
}
