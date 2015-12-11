package org.scu.spark.scheduler

/**
 * 存储当前RDD的信息，用于将stage的信息传播给sparkListener
 * Created by bbq on 2015/12/10
 */
class StageInfo (
                val stageId:Int,
                val attempteId:Int,
                val name:String,
                val numTasks:Int,
                //TODO RDDinfo
                val parentIds:Seq[Int],
                val details:String,
                private[spark] val taskLocalityPreferences :Seq[Seq[TaskLocation]] = Seq.empty
                  ){
  /**从DAGScheduler向taskscheduler提交的时间*/
  var _submissionTime : Option[Long] = None


}

private[spark] object StageInfo{
  /**
   * 根据一个stage构建一个stageInfo
   */
  def fromStage(
               stage:Stage,
               attemptId:Int,
               numTasks:Option[Int] = None,
               taskLocalityPreferences : Seq[Seq[TaskLocation]] = Seq.empty
                 ):StageInfo={
    //TODO rddInfos
    new StageInfo(
    stage.id,
    attemptId,
    //TODO rdd callsite name
    "TODO callsite",
    numTasks.getOrElse(stage.numTask),
    stage.paraents.map(_.id),
    //TODO stage callsite detail
    "todo callsite detail",
    taskLocalityPreferences
    )
  }

}