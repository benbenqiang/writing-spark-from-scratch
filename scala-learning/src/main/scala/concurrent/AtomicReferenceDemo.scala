package concurrent

import java.util.concurrent.atomic.AtomicReference

/**
 * 与volatile 不同的是，atomic将get和set作为了一个原子操作，并提供CAS（compare and set）
 * Created by bbq on 2015/12/15
 */
object AtomicReferenceDemo {
  def main(args: Array[String]) {
    @volatile val p1 = Person(101)
    val p2 = Person(102)
    val ar = new AtomicReference(p1)
    ar.compareAndSet(p1, p2)
    println(ar.get())
  }
}

case class Person(id: Long)