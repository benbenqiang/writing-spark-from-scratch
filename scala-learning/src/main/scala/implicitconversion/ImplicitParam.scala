package implicitconversion

/**
 * 函数或方法可以带有一个标记为implicit的参数列表。这种情况下，编译器会查找缺省值。
 * Created by bbq on 2015/12/6
 */
class ImplicitParam {
  def quote(what: String)(implicit delims: Delimiters) =
    println(delims.left + what + delims.right)

  /**利用隐式参数进行隐式转换
    * 我们并不知道T是否具有<这个方法，因此我们提供一个转换函数来达到目的。
    */
  def smaller[T](a:T,b:T)(implicit order : T => Ordered[T]):T={
    if(a < b ) a else b
  }
  def demo1() = {
    quote("快学scala")(Delimiters("<",">"))
  }
  def demo2()={
    implicit val qutoDelimiters = Delimiters("<<--","-->>")
    quote("快学scala")
  }

  def demo3()={
    implicit def A2Orderd(a:A): Ordered[A] =new Ordered[A]() {override def compare(that: A): Int = 1}
    smaller(new A,new A)
  }
}

object ImplicitParam extends App{
  new ImplicitParam().demo1()
  new ImplicitParam().demo2()

}

case class Delimiters(left: String, right: String)

