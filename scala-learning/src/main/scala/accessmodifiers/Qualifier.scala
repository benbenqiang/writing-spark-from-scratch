package accessmodifiers {
package packageone {

/**
 * scala中只有默认public，protected，private
 * public:在任意地方都可以调用
 * protected：只有本身和子类对象中才可调用，不同包的子类也可以，加入作用域，指定作用域则变为public
 * private: 只有本身类才能调用,private[this]只能自身对象可以调用
 *
 * 与java 不同的是：java有default，而scala没有，scala可以指定作用域，更加灵活
 * Created by bbq on 2015/12/1
 */
class Qualifier {
  val public_name = "习近平"
  protected val protected_name = "习大大"
  protected[accessmodifiers] val protected_name_2 = "习大大"
  private val private_name = "平平"
  private[this] val private_this_name = "小平平"

  def compare(other:Qualifier)={
    other.private_name
    /**other.private_this_name,this只能对象内使用*/
  }
}

class QualifierChild extends Qualifier {
  public_name
  protected_name
  /**private_name 不可访问*/
}

class Other {
  val obj = new QualifierChild()
  obj.public_name
  /**obj.protected_name与private_name 不可访问,protected只能在子类，private只能在自身对象*/
}

}

/**另外一个包*/
package packagetwo {

import accessmodifiers.packageone.Qualifier


/**不同包中的子类可以访问protected和public*/
class QualifierChild extends Qualifier {
  public_name
  protected_name
}
/**不同包中的其他类可以访问protected[本包名]和public*/
class Other{
  val obj = new QualifierChild()
  obj.public_name
  obj.protected_name_2
}
}

}


