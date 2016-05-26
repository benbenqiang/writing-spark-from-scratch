package org.scu.spark.storage

import org.scu.spark.Logging

import scala.reflect.ClassTag

/**
 * BlockInfo 维护了每个Block的元数据
 * BlockInfo并不是线程安全性的，但是可以通过BlockInfoManager维护
 * Created by applelab on 2016/5/11.
 */
private[storage] class BlockInfo(
                                val level:StorageLevel,
                                val classTag:ClassTag[_],
                                val tellMaster:Boolean
                                  ) {
  /**blcok的size*/
  private[this] var _size : Long = 0
  def size : Long = _size
  def size_=(s:Long) = {
    _size = s
    checkInvariants()
  }

  /**当读block被锁住的次数*/
  private[this] var _readerCount : Int = 0
  def readerCount : Int = _readerCount
  def readerCount_=(c:Int) ={
    _readerCount = c
    checkInvariants()
  }

  /**当写block被锁住的attemp id*/
  private[this] var _writeTask:Long = BlockInfo.NO_WRITER
  def writerTask:Long = _writeTask
  def writerTask_=(t:Long) = {
    _writeTask = t
    checkInvariants()
  }

  private def checkInvariants():Unit = {
    /**bloc 读计数必须大于0*/
    assert(_readerCount > 0)
    /**一个块当前要么被读锁，要么写锁*/
    assert(_readerCount ==0 || _writeTask == BlockInfo.NO_WRITER)
  }

  //checkInvariants()
}

private[storage] object BlockInfo{
  val NO_WRITER:Long  = -1
  val NON_TASK_WRITER:Long = -1024
}

/**
  * 作为BlockManager的组件，管理跟踪每个Block的元数据信息，并且管理Block的读写锁
  */
private[storage] class  BlockInfoManager extends  Logging{

}
