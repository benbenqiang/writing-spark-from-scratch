package org.scu.spark

import org.scu.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by bbq on 2015/11/16
 */
abstract class Dependency[T]{
  def rdd:RDD[T]
}

abstract class NarrowDependency[T](_rdd:RDD[T]) extends Dependency[T]{
  /**
   * 窄依赖：子RDD的partition中的数据来源于父RDD确定的一个或几个partition
   * 可能是OneToOne也有可能是RangeDependency
   * @param partionID 子RDD的partitionID
   * @return 依赖的父RDD的partition的ID
   */
  def getParents(partionID:Int):Seq[Int]

  override def rdd:RDD[T] = _rdd

}

class ShuffleDependency[K:ClassTag,V:ClassTag,C:ClassTag](
                                                         val _rdd:RDD[(_,_)],
                                                         val partitioner: Partitioner
                                                           )
class OneToOneDependency[T](_rdd:RDD[T]) extends NarrowDependency[T](_rdd){
  /**
   * 子partitionID就是父的ID
   */
  override def getParents(partitionID: Int): Seq[Int] = Seq(partitionID)
}
