package org.scu.spark.rdd

import org.scu.spark.{Partition, SparkContext}

/**
 * 通过sc.parallelize创建的RDD类型
 * Created by bbq on 2015/11/16
 */
class ParallelCollectionRDD[T](sc: SparkContext,
                               @transient seq: Seq[T],
                               numSlices: Int) extends RDD[T](sc, Nil) {

  override def getPartition: Array[Partition] = {
    //先将数据分片
    val slice = ParallelCollectionRDD.slice(seq, numSlices)
    //将每个数据分片封装成Partition
    slice.indices.map(i =>new ParallelCollectionPartition[T](id,i,slice(i))).toArray
  }

  override def compute(split: Partition): Iterator[T] = {
    split.asInstanceOf[ParallelCollectionPartition[T]].iterator
  }

}

object ParallelCollectionRDD {
  def slice[T](seq: Seq[T], numSlice: Int): Seq[Seq[T]] = {
    seq.grouped(numSlice).toSeq
  }
}

class ParallelCollectionPartition[T](
                                      val rddID: Long,
                                      var slice: Int,
                                      var values: Seq[T]
                                      ) extends Partition {
  override def index: Int = slice

  def iterator :Iterator[T] = values.iterator
}