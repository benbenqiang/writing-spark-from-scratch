package classbasic

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

/**
 * 问：akka中为什么用case class？
 * 1.case class 实现了hashcode和equals，用于模式匹配
 * 2.默认实现了序列化
 * Created by bbq on 2015/11/11
 */
object CaseClassDemo {
  def main(args: Array[String]) {
    val a1 = new A("1")
    val a2 = new A("1")
    val ca1= CA("1")
    val ca2= CA("1")

    println(a1==a2) //返回false
    println(ca1==ca2) //返回true

    val oos = new ObjectOutputStream(new ByteArrayOutputStream())

    oos.writeObject(ca1) //成功，因为默认实现了序列化
    oos.writeObject(a1)  //Exception in thread "main" java.io.NotSerializableException: A
  }
}
case class CA(s:String)
class A(s:String)