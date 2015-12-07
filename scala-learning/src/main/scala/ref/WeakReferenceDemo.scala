package ref

import scala.ref.SoftReference

import scala.collection.mutable.ArrayBuffer
import scala.ref.WeakReference

/**
 *  特性：如果一个对象只有WeakReference引用，那么这个对象就可以被垃圾回收器回收
 *  背景：map或者数组持有对象引用导致不能被GC回收，造成内存泄露。
 *
 *  StrongReference > SoftReference > WeakReference
 * 参考博客：http://blog.csdn.net/devilkin64/article/details/7916630
 * Created by bbq on 2015/12/7
 */
class WeakReferenceDemo {
  /**
   * 以弱引用的方式向数组中添加对象，当gc时，会被虚拟机回收
   */
  def fillHeapWithWeakReference(num:Int)={
    val list = ArrayBuffer[WeakReference[OOMObject]]()
    for(i <- 0 to num){
      Thread.sleep(1)
      list.append(WeakReference(new OOMObject()))
      println(i)
    }
  }

  /**
   * 以强引用的方式向数组中添加对象，当gc时，不会被回收，最终OOM
   */
  def fillHeapWithStrongReference(num:Int)={
    val list = ArrayBuffer[OOMObject]()
    for(i <- 0 to num){
      Thread.sleep(100)
      list.append(new OOMObject())
      println(i)
    }
  }

  /**
   * 以软引用的方式向数组中添加对象，当普通gc时，不会被回收，当虚拟机压力较大时，就会被回收，适用于cache类型的应用
   */
  def fillHeapWithSoftReference(num:Int)={
    val list = ArrayBuffer[SoftReference[OOMObject]]()
    for(i <- 0 to num){
      Thread.sleep(2)
      list.append(new SoftReference(new OOMObject()))
      println(i)
    }
  }

}

object WeakReferenceDemo extends App{
//   new WeakReferenceDemo().fillHeapWithWeakReference(100000)
//  new WeakReferenceDemo().fillHeapWithStrongReference(100000)
  new WeakReferenceDemo().fillHeapWithSoftReference(100000)
}

class OOMObject{
  val array = new Array[String](64*1024)
}
