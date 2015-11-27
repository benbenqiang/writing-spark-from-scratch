package org.scu.spark

/**
 * spark中的累加器实现：
 *
 * Created by bbq on 2015/11/27
 */
object Accumulators {

}

/**
 * 累加器的帮助类，实现如何对元素进行相加，以及对两个相加后的集合进行合并操作。类似于aggregate
 * @tparam R 最终的计算的数据类型
 * @tparam T 中间结果，部分相加的结果
 */
trait AccumulableParam[R,T] extends Serializable{

  /**
   * 添加一个中间结果类型
   * 例如：我们需要将所有的Int添加到List[Int]中，就要使用addAccumulator(List,Int)
   */
  def addAccumulator(r:R,t:T):R

  /**
   * 将两个计算结果类型的数据合并
   */
  def addInPlace(r1:R,r2:R):R
}