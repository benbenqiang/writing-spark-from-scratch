package io.serialize

import java.io._

/**
 * Serializable 和 Externalizable的区别： Ser会将类中全部的属性都序列化，但是这样会带来性能问题。Externalizable可以定制化哪些需要序列化哪些不需要。
 * Ser其实也可以定制，但是并不是强制要求，实现了Exter的类必须实现readExternal和writeExternal。Exter在反序列化的时候会调用类的默认构造函数，进行初始化。
 *
 * 序列化的时候跳过对trasient和静态变量保存
 * Created by bbq on 2016/5/3.
 */
object ExternalizableDemo {
  def main(args: Array[String]) {
    val isSeri = false
    val fileSep = File.separator
    val file = new File(s"scala-learning${fileSep}src${fileSep}main${fileSep}resources${fileSep}ExternalizableDemo.data")
    if(isSeri){
      val a = new A()
      val b = new B()
      val oos = new ObjectOutputStream(new FileOutputStream(file))
      println("序列化对象")
      A.english = 100
      oos.writeObject(a)
      oos.writeObject(b)
    }

    println("反序列化A")
    val ois = new ObjectInputStream(new FileInputStream(file))
    val newA = ois.readObject().asInstanceOf[A]
    println("正常变量："+ newA.age)
    /**静态变量不会被*/
    println("静态变量："+ A.english)

    println("反序列化B")
    val newB = ois.readObject().asInstanceOf[B]


  }
}

class A extends Serializable {
  println("A 的主构造函数")
  var age = 10
}
object A  {
  var english = {
    println("初始化静态变量english=10")
    10
  }
}

class B extends Externalizable{
  println("B的构造函数")
  override def readExternal(in: ObjectInput): Unit = {
    println("read External")
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    println("writeExternal")
  }
}