package typeSystem

import scala.reflect.ClassTag

/**
 * 与类型系统相关的知识。
 * scala 获取一个类的Class信息，有两种方法：
 * class A
 * val a = new A
 * 1. a.getClass : 得到的是Class[A]的某个子类 Class[ _ <: A]
 * 2. classOf[A] : 得到的就是正确的Class[A] 相当于java中的 T.class
 * 注意java里的 Class[T]是不支持协变的，所以无法把一个 Class[_ < : A] 赋值给一个 Class[A] , 但是支持逆变
 *
 * 相关链接：http://blog.csdn.net/wsscy2004/article/details/38440247
 * Created by bbq on 2016/5/5
 */
object typeSystemDemo {
  class A

  /**获取Class信息的方式，以及两种的区别*/
  def getClassDemo(): Unit={
    val a = new A
    val class1: Class[_ <: A] = a.getClass
    val class2: Class[A] = classOf[A]
    /**相等，虽然两个类型不同，但是只想的Class是相同的*/
    println(class1  == class2)
    /**支持逆变，不支持协变*/
    //val class3 : Class[A] = class1
    val class4 : Class[_ <: A] = class2
  }

  /**类型一致的对象它们的类也是一致的，反过来，类一致的，其类型不一定一致。*/
  def typeAndClassDemo: Unit ={
    import scala.reflect.runtime.universe._
    println(classOf[List[Int]] == classOf[List[String]])
    println(typeOf[List[Int]] == typeOf[List[String]])
  }

  /**Array在初始化的时候需要类信息*/
  //def mkArray[T](elems:T*) = Array(elems:_*)
  /**ClassTag[T]保存了被泛型擦除后的原始类型T,提供给运行时的。*/
  def mkArray[T:ClassTag](elems:T*) = Array(elems:_*)

  def main(args: Array[String]) {

  }
}

