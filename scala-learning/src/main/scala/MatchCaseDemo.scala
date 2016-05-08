/**
 * Created by bbq on 2016/5/8
 */
object MatchCaseDemo {
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
