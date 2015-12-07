package implicitconversion


/**
 * 隐式转化基本使用.
 * 基本概念：
 *   所谓隐式转化，指的是那种以implicite关键字声明的带有单个参数的函数。例如Demo1
 *   利用隐式转换和隐式参数，你可以提供优雅的类库，对类库的使用者隐藏掉那些枯燥的细节。
 *
 * 以下条件下会转化：
 * 1.当参数类型或表达式类型与预期不同，需要转化在能满足
 * 2.访问一个不存在的成员
 *
 * 以下条件不会使用转换：
 * 1.若不在隐式转换的情况下也能编译通过，那么就不会尝试隐式转换。
 * 2.编译器不会尝试同时执行多个转换。比如convert1(convert2(a))*b
 * 3.存在二义性的转换是个错误。如果convert1（a)和convert2（a）都是合法的，会报错
 *
 *
 *
 * 参考《快学scala》隐式转化
 * Created by bbq on 2015/11/30
 */
object BasicTansform {
  def demo1()={
    /**定义一个隐式转换*/
    implicit def A2RichA(a:A): RichA = new RichA(a)
    val a = new A
    /**A拥有了RichA的方法，就像RDD拥有PairRDD的方法一样*/
    a.prt()
  }

  def main(args: Array[String]) {
    import scala.language.implicitConversions
    demo1()
  }
}

class A{
  def prt()  = print("not rich")
}
class RichA(a:A){
   def prt(): Unit ={
    println("so rich")
  }
}

