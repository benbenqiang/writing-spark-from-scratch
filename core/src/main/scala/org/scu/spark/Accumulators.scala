package org.scu.spark

import java.util.concurrent.atomic.AtomicLong

import scala.ref.WeakReference

class Accumulable[R,T] private[spark](
                                     initialValue:R,
                                     param:AccumulableParam[R,T],
                                     val name : Option[String],
                                     internal : Boolean
                                       ) extends Serializable{
  val id  = Accumulators.newId()

  /**当前的值*/
  @volatile @transient private var value_ : R = initialValue

  val zero = param.zero(initialValue)

  /**时候在主节点上*/
  private var deserialized = false

  /**如果是internal的时候，需要向driver发送心跳信息*/
  private[spark] def isInternal:Boolean = internal

  def += (term:T) { value_ = param.addAccumulator(value_,term) }

  def add(term:T) { value_ = param.addAccumulator(value_,term) }

  def ++= (term:R) { value_ = param.addInPlace(value_,term) }

  def merge (term:R) {value_ = param.addInPlace(value_,term) }

  /** 在task任务中是不能获取全局值的*/
  def value:R={
    if(!deserialized)
      value_
    else
      throw new UnsupportedOperationException("Can't read accumulator value in task")
  }

  def localValue : R = value_

  /**重新设置value，只有driver才可以 */
  def value_= (newValue:R): Unit ={
    value = newValue
  }

  /**重新设置value，只有driver才可以设置*/
  def setValue(newValue:R): Unit ={
    value = newValue
  }

  override def toString :String = if (value_ == null) "null" else value_.toString

}


/**
 * 累加器的帮助类，实现如何对元素进行相加，以及对两个相加后的集合进行合并操作。类似于aggregate
 * @tparam R 最终的计算的数据类型
 * @tparam T 中间结果，部分相加的结果
 */
trait AccumulableParam[R,T] extends Serializable{

  /**
   * 添加一个中间结果类型
   * 例如：我们需要将所有的List[Int]添加到Int中，就要使用addAccumulator(Int,List[Int])
   */
  def addAccumulator(r:R,t:T):R

  /**
   * 将两个计算结果类型的数据合并
   */
  def addInPlace(r1:R,r2:R):R

  /**
   * 返回初始化值，如果R的类型是List[Int](n),那么就初始化为值全为0的N维List
   */
  def zero(initialValue:R):R
}

/**
 * spark中的累加器实现：精简版实现，当中间结果和最终结果的类型相同时。
 * 可以用来当作计数器。spark原生支持数字类型相加，也可以自己实现。
 *
 * 但我们是用Accumulator的时候，是用SparkContext.accumulator。
 * Task在运行的时候可以相加，但是不能读取数据，最终的数据只能由master读取
 *
 *
 * Created by bbq on 2015/11/27
 */
class Accumulator[T] private[spark](
                                   @transient private[spark] val initialValue : T,
                                   parm : AccumulatorParam[T],
                                   name:Option[String],
                                   internal:Boolean
                                     )extends Accumulable[T,T](initialValue,parm,name,internal){
  def this(initialValue:T,param:AccumulatorParam[T],name:Option[String])={
    this(initialValue,param,name,false)
  }

  def this(initialValue:T,param:AccumulatorParam[T])={
    this(initialValue,param,None,false)
  }

}

object Accumulators extends Logging{

  /**
   * 之所以采用弱引用是因为，orginals需要维护一个accumulators的所有实例，
   * 但是如果此处用强引用则会导致内存泄露，所有当RDD使用完某个accumulator并且移除了
   * 强引用，那么这里便会自动被GC收回
   */
  val originals = collection.mutable.Map[Long,WeakReference[Accumulable[_,_]]]()

  private val lastId = new AtomicLong(0)

  def newId():Long = lastId.getAndIncrement()

  def register(a:Accumulable[_,_]) = synchronized{
    originals(a.id) = new WeakReference[Accumulable[_, _]](a)
  }

  def remove(accId:Long): Unit ={
    synchronized{
      originals.remove(accId)
    }
  }

  /**根据ID，想现有的accumulator添加值*/
  def add(values:Map[Long,Any])=synchronized{
    for((id,value) <- values){
      if(originals.contains(id)){
        originals(id).get match {
          case Some(accum) => accum.asInstanceOf[Accumulable[Any,Any]] ++= value
          case None => throw new IllegalAccessError("Attempted to access garbage collected Accumulator")
        }
      }else{
       logWarning("Ignoring accumulator update for unknown accumulator id" + id)
      }
    }
  }


}

/**
 * R和T的类型相同
 * @tparam T 中间结果，部分相加的结果
 */
trait AccumulatorParam[T] extends AccumulableParam[T,T]{
  def addAccumulator(t1:T,t2:T):T={
    addInPlace(t1,t2)
  }
}

/**
 * 隐式参数，默认实现的四种类型
 */
object AccumulatorParam{
  implicit object DoubleAccumulatorParam extends AccumulatorParam[Double]{
    override def addInPlace(r1: Double, r2: Double): Double = r1 + r2
    override def zero(initialValue: Double): Double = 0.0
  }

  implicit object IntAccumulatorParam extends AccumulatorParam[Int] {
    override def addInPlace(r1: Int, r2: Int): Int = r1 + r2
    override def zero(initialValue: Int): Int = 0
  }

  implicit object LongAccumulatorParam extends AccumulatorParam[Long]{
    override def addInPlace(r1: Long, r2: Long): Long = r1 + r2
    override def zero(initialValue: Long): Long = 0L
  }

  implicit object FloatAccumulatorParam extends AccumulatorParam[Float]{
    override def addInPlace(r1: Float, r2: Float): Float = r1 + r2
    override def zero(initialValue: Float): Float = 0f
  }
}

private[spark] object InternalAccumulator {
  val PEAK_EXECUTION_MEMORY = "peakExecutionMemory"

  /**
   * 用于跟踪内部metrics的accumulators
   *
   * 在stage中创建，stage中的所有tasks都使用
   */
  def create(sc:SparkContext) : Seq[Accumulator[Long]] ={
    /**
     * 在shuffle aggregation join时，为了大概估计这些操作整体所占内存的峰值。
     */
    Seq (
    new Accumulator(0L,AccumulatorParam.LongAccumulatorParam,Some(PEAK_EXECUTION_MEMORY),internal = true)
    )
  }
}