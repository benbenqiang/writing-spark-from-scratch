package org.scu.spark.storage

import java.io.{ObjectInput, ObjectOutput, Externalizable}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.immutable.HashSet

/**
 * block data 存储策略.
 * 序列化的时候为位存储的策略，读取和存储策略用按位与和按位或，相当不错
 * Created by bbq on 2016/5/9
 */

/**柱构造器私有*/
class StorageLevel private(
                            private var _useDisk : Boolean,
                            private var _useMemory : Boolean,
                            private var _useOffHeap : Boolean,
                            private var _deserialized : Boolean,
                            private var _replication : Int =1) extends Externalizable{

  private def this(flags:Int,replication:Int)={
    this((flags & 8 ) != 0,(flags & 4) != 0,(flags & 2) != 0,(flags & 1 ) != 0,replication)
  }

  /**默认策略，只使用内存*/
  def this() = this(4,1)

  def useDisk : Boolean = _useDisk
  def useMemory : Boolean = _useMemory
  def useOffHeap : Boolean = _useOffHeap
  def deserialized : Boolean = _deserialized
  def replication : Int = _replication

  assert(replication < 40 , "Replication restricted to be less than 40 for calculating hash code")

  override def clone() : StorageLevel = {
    new StorageLevel(useDisk,useMemory,useOffHeap,deserialized,replication)
  }

  override def equals(other:Any):Boolean = other match {
    case s : StorageLevel =>
      s.useDisk == useDisk &&
      s.useMemory == useMemory &&
      s.useOffHeap == useOffHeap &&
      s.deserialized == deserialized &&
      s.replication == replication
    case _ => false
  }

  /**必须是使用内存或者磁盘，并且分片个数大于零*/
  def isValid : Boolean = (useMemory || useDisk) && (replication >0)

  /**将存储策略按照位策略转化成Int*/
  def toInt : Int = {
    var ret = 0
    if(_useDisk)
      ret |= 8
    if(_useMemory)
      ret |= 4
    if(_useOffHeap)
      ret |= 2
    if(_deserialized)
      ret |= 1
    ret
  }

  override def readExternal(in: ObjectInput): Unit = {
    val flags = in.readByte()
    _useDisk = (flags & 8) != 0
    _useMemory = (flags & 4 ) != 0
    _useOffHeap = (flags & 2) != 0
    _deserialized = (flags & 1) != 0
    _replication = in.readByte()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    /**将Int的高24位忽略，节省空间*/
    out.writeByte(toInt)
    out.writeByte(_replication)
  }

  override def toString :String = {
    s"StorageLevel (disk=$useDisk, memory=$useMemory, offheap=$useOffHeap, "+
    s"deserialized=$deserialized, replication=$replication)"
  }

  override def hashCode() : Int = toInt * 41 + replication
}

object StorageLevel {
  val NONE = new StorageLevel(false, false, false, false)
  val DISK_ONLY = new StorageLevel(true, false, false, false)
  val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
  val MEMORY_ONLY = new StorageLevel(false, true, false, true)
  val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
  val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
  val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
  val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
  val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
  val OFF_HEAP = new StorageLevel(true, true, true, false, 1)

  def apply(
           useDisk:Boolean,
           useMemory:Boolean,
           useOffHeap:Boolean,
           deserialized:Boolean,
           replication:Int
             ) : StorageLevel ={
    getCachedStrageLevel(new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication))
  }

  /**缓存当前出现过的storageLevel防止重读的创建节省内存*/
  private[spark] val storageLevelCache = new ConcurrentHashMap[StorageLevel,StorageLevel]()

  private[spark] def getCachedStrageLevel(level:StorageLevel):StorageLevel ={
    storageLevelCache.putIfAbsent(level,level)
    storageLevelCache.get(level)
  }
}
