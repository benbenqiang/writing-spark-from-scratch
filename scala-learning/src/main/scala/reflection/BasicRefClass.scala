package reflection


/**
  * scala中使用反射。需要注意的地方
  * Created by bbq on 2016/5/23
  */
class BasicRefClass(var name:Boolean) {
  var age : Int = 0
  def this(age:Int){
    this(true)
    this.age = age
  }

  def this(name:Boolean,age:Int){
    this(name)
    this.age = age
  }
}

object BasicRefClass{
  def main(args: Array[String]) {
    /**通过反射获取所有构造函数的时候，newInstance必须用java.lang*/
    classOf[BasicRefClass].getConstructor(classOf[Boolean]).newInstance(new java.lang.Boolean(true))

  }
}