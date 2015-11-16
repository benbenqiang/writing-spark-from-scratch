package org.scu.spark.rdd

import org.scu.spark.{Partitioner, Partition}

/**
 * Created by bbq on 2015/11/16
 */

class MapPartitionsRDD[U, T](
                              prev: RDD[T],
                              f: (Int, Iterator[T]) => Iterator[U] //partitionID,iterator
                              ) extends RDD[U](prev) {

  override def compute(split: Partition): Iterator[U] = f(split.index,firstParent[T].iterator(split))

  /**
   * 使用上一个RDD的partitions
   */
  override def getPartition: Array[Partition] = firstParent[T].getPartition

  override val partitioner: Option[Partitioner] = None
}
