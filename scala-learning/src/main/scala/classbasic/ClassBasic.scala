package classbasic

import scala.collection.mutable.ArrayBuffer

/**
 * 知识点：
 * 1.scala 每个类不用生命public，都具有可见性
 * 2.取值器改值器编写风格
 * 3.scala生成面向JVM的类，会自动为属性生成 getter和setter
 * 4.也可以重新定义setter getter，实现统一访问原则
 * 5.修饰符：private 可以被类本身调用，另外一个相同对象的私有字段也是可以的
 *          若加上private[this]，对象私有,不会生成gettersetter，类私有会生成
 * 6.构造参数中不带val 或 var ，若至少被一个方法使用，则升格为val private[this]字段,如果未被使用则作为参数
 * 7.主构造器可以变成私有，用户只能使用从构造器
 * 参考资料：《快学scala》第五章
 * Created by bbq on 2015/11/23
 */
object ClassBasic {
  def counter() = {
    val myCounter = new Counter()
    /** 改值器加括号 */
    myCounter.increment()
    /** 取值器不加括号 */
    println(myCounter.current)
  }

  def person() = {
    val person = new Person()
    person.age = 10

    val person2 = new Person2(20)
    /**统一访问原则*/
    person2.age=10
    println(person2.age)

  }

  def main(args: Array[String]): Unit = {
    person()
  }
}

/** 知识点：1-2 */
class Counter {
  /** 必须初始化 */
  private var value = 0

  /** 默认public */
  def increment() {
    value += 1
  }

  def current = value
}

/**
 * 知识点：3-7
 * 自动生成setter，使用javap查看字节码文件：
 * setter：public void age_$eq<int>; 等号被翻译成$eq，JVM方法名不允许'='出现
 * getter：public int age();
 */
class Person {
  var age = 0
}
/**私有主构造器*/
class Person2 private {
  private var privateAge = 0

  /** 重新定义getter和setter*/
  def age = privateAge

  def age_=(newValue: Int): Unit = {
    /** 不能变得再年轻*/
    if (newValue > privateAge) privateAge = newValue
  }

  def this(age:Int)={
    this()
    this.privateAge = age
  }
}

class Network{
  class Member(val name:String){
    val contacts = new ArrayBuffer[Member]()
  }

  private val members = new ArrayBuffer[Member]

  def join(name:String)={
    val m = new Member(name)
    members += m
    m
  }
}