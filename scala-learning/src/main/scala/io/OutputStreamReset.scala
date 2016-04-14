package io

import java.io.{File, FileOutputStream, ObjectOutputStream}

/**
 * OutputStream中有一个reset方法，对于长连接流导致的内存溢出.
 * 在spark的javaSerializer里使用了reset。
 * 若调用了reset之后，就会放弃已经写入对象的引用，但是同时丢失了类信息，对于重复类的类信息会重复写入流中。
 * 相关链接： http://www.ibm.com/developerworks/cn/java/j-lo-streamleak/
 * Created by bbq on 2016/4/14
 */
object OutputStreamReset {
  def main(args: Array[String]) {
    val fileSep = File.separator
    val file = new File(s"scala-learning${fileSep}src${fileSep}main${fileSep}resources${fileSep}OutputStreamReset.data")
    val out = new ObjectOutputStream(new FileOutputStream(file))

    out.writeObject(Person("bbq11", 11))
    out.reset()
    out.writeObject(Record(1.1, 2.2))
    out.reset()
    out.writeObject(Person("bbq22", 12))
    out.reset()
    out.writeObject(Record(3.3, 4.4))
    out.reset()
    out.writeObject(Person("bbq33", 13))
    out.close()
  }
}

case class Person(name: String, age: Int)

case class Record(math: Double, english: Double)