package org.scu.spark

/**
 * Created by bbq on 2015/11/24
 */
class StorageLevel private(
                            private var _useDisk: Boolean,
                            private var _useMemory: Boolean,
                            private var _useOffHeap: Boolean,
                            private var _deserialized: Boolean,
                            private var _replication: Int = 1
                            )
  extends Serializable {

  /**利用按位与的方式决定缓存方式，而不用传入多个参数*/
  private def this(flags:Int,replication:Int){
    this((flags & 8) != 0,(flags & 4) != 0,(flags & 2) != 0,(flags & 1) != 0,replication)
  }

  /**默认使用memory缓存*/
  def this() = this(false,true,false,false)

  /**对外只能获取数据，不能修改*/
  def useDisk = _useDisk
  def useMemory = _useMemory
  def useOffHeap = _useOffHeap
  def deserialized = _deserialized
  def replication = _replication

  override def clone(): AnyRef = {
    new StorageLevel(useDisk,useMemory,useOffHeap,deserialized,replication)
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case s :StorageLevel=>
        s.useDisk == useDisk &&
        s.useMemory == useMemory &&
        s.useOffHeap == useOffHeap &&
        s.deserialized == deserialized &&
        s.replication == replication
      case _ =>
        false
    }
  }

  /** |= 按位与后赋值*/
  def toInt:Int = {
    var ret = 0
    if(_useDisk) ret |= 8
    if(_useMemory) ret |= 4
    if(_useOffHeap) ret |= 2
    if(_deserialized) ret |= 1
    ret
  }

  override def toString: String =s"StorageLevel($useDisk,$useMemory,$useOffHeap,$deserialized,$replication)"


  override def hashCode(): Int = toInt * 41 + replication

  /**验证是否合法，必须有一种缓存策略，并且replication > 0*/
  def isValid = (useMemory || useDisk || useOffHeap) && (replication > 0)

  def description :String={
    var result = ""
    result += (if(useDisk) "Disk" else "")
    result += (if (useMemory) "Memory " else "")
    result += (if (useOffHeap) "ExternalBlockStore " else "")
    result += (if (deserialized) "Deserialized " else "Serialized ")
    result += s"${replication}x Replicated"
    result
  }
}

object StorageLevel{
  /**常用的存储等级*/
  val NONE = new StorageLevel(false,false,false,false)
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
  val OFF_HEAP = new StorageLevel(false, false, true, false)
}
