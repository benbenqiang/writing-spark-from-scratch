package implicitconversion

/**
 * Created by bbq on 2015/12/6
 */
class ContextImplicit {

}

object ContextImplicit extends App{
  implicit object AOrdering extends Ordering[A]{
    override def compare(x: A, y: A): Int = 1
  }
  new Pair[A](new A,new A)
}

/**
  * 要求作用域中存在一个类型为M[T]的隐式值。
 *  import Orderd._接受一个隐式的M[T]，并将T转换为Ordered[T]。
  */
class Pair[T:Ordering](val first:T,val second:T){
  import Ordered._
  def smaller()=if(first < second) first else second
}
