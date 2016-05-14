/**
 * Created by bbq on 2016/5/8
 */
object MatchCaseDemo {
  def main(args: Array[String]) {
    ereaseType
  }

  /**类型擦除对match的影响*/
  def ereaseType: Unit = {
    val map: Map[Int, Int] = Array((1, 1)).toMap
    val array: Array[Int] = Array(1)
    val matchObject: Array[Any] = Array(map, array)
    matchObject.foreach{
      case m:Map[String,String] =>println("Map[Int,Int] object 错误，不应该被匹配")
      case array:Array[String] => println("Array[Int] object")
      case _ => println("no match")
    }
  }

  /**match 除了可以匹配对象，还可以匹配类型*/
  def basicMatch : Unit = {
    case class A(name:String)
    val matchObject: Array[Any] = Array(A,A("heheda"),Int,1)
    matchObject.foreach {
      case Int => println("Int class ")
      case _: Int => println("Int object")

      /**下面这三种方式都可以捕获一个case class 对象*/
      case aObj @ A(name) => println("A Object")
      case aObj : A => println("A Object")
      case A(name) => println("A Object")

      case A => println("class A")
      case _ => println("no match" )
    }
  }

  /**match 匹配case class*/
  def caseclassWithMatch: Unit ={
    case class RDD(string1:String,string2:String,string3:String)
    val testRdd = RDD("1","2","3")
    testRdd match {
      case RDD("1","2","3") =>
        println(true)
      case _ =>
        println("no match")
    }

  }

  /**注：如果有两个参数的RDD类，这里就会报错的*/
  def regexWithMatch ={
    val RDD = "rdd_([0-9]+)_([0-9]+)".r
    val stringRdd = "rdd_10_10"
    stringRdd match {
      case RDD(rddId,splitIndex) =>
        println(rddId + splitIndex)
      case _ =>
        println("no match")
    }
  }
}
