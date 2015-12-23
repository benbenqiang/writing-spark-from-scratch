package classbasic

import scala.collection.mutable.ArrayBuffer

/**
 * Created by bbq on 2015/12/23
 */
object CaseClassCopy {
  /**cass class copy 是浅拷贝*/
  def demo1()={
    val array1 = new ArrayBuffer[String]
    array1.append("123")
    val copy1 = CopyTest("name1",array1)
    val copy2 = copy1.copy(name = "name2")
    copy1.array.append("456")
    println(copy2.array.mkString(","))
  }

  def main(args: Array[String]) {
    demo1()
  }
}


case class CopyTest(name:String,array:ArrayBuffer[String])