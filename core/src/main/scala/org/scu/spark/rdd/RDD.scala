package org.scu.spark.rdd

import org.scu.spark._

/**
 * 弹性分布式数据集，RDD的五个主要特性：
 * 1.分片：数据会被分片存储在不同的机器上。
 * 2.依赖: 每个RDD都会记录由哪个RDD演化而来的
 * 3.计算：每个RDD都有计算函数用于处理每个分片
 * 4.partioner：决定每条记录应该被分配到哪些机器上
 * 5.preferedlocation：分布式框架的精髓是：移动计算而不移动数据
 *
 * Created by bbq on 2015/11/16
 */
abstract class RDD[T](
                   @transient private var _sc:SparkContext,
                   @transient private var deps:Seq[Dependency[_]] ) extends Serializable with Logging{

  val partitioner : Option[Partitioner] = None

  val id: Int = _sc.newRddId()

  /**
   * 记录创建该RDD的sparkcontext
   */
  def context = _sc

  def this(parent:RDD[_]) = this(parent.context,List(new OneToOneDependency[_](parent[_])))

  /**
   * 最近一个父RDD
   */
  def firstParent[U]:RDD[U] =  deps.head.rdd.asInstanceOf[RDD[U]]

  def getPartition:Array[Partition]

  def compute(split:Partition):Iterator[T]

  def iterator(split:Partition):Iterator[T]={
    compute(split)
  }

  def map[U](f: T => U) :RDD[U]={
    new MapPartitionsRDD[U,T](this,(id,iter)=>iter.map(f))
  }

}
