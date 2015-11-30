package implicitconversion

import classbasic.A

/**
 * 隐式转化基本使用.
 * 基本概念：
 *   所谓隐式转化，指的是那种以implicite关键字声明的带有单个参数的函数。例如Demo1
 * Created by bbq on 2015/11/30
 */
object BasicTansform {
  def demo1()={
    implicit def A2Rich(a:A): RichA = new RichA(a)
    val a = new A
    /**A拥有了RichA的方法，就像RDD拥有PairRDD的方法一样*/
    a.print()
  }

  def main(args: Array[String]) {
    import scala.language.implicitConversions
    demo1()
  }
}

class A{
  def printt()  = print("123")
}
class RichA(a:A){
  def print(): Unit ={
    println("so rich")
  }
}

