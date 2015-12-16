package concurrent

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Promise, future}

/**
 * futures是为了一个还没有存在的结果，而当成一种只读占位符的对象去创建，那么promse
 * 就是一个可写的，可实现一个future的单一复制容器。
 *
 * 相当于future中不用return结果，将结果赋给promise
 *
 * 每个promise指定一个futures，可以为该futures赋值，赋值对应的success和failed
 *
 * Created by bbq on 2015/12/16
 */
object PromiseDemo {
  def demo1() = {
    val p = Promise[String]()
    val f = p.future

    val producer = future {
      println("start producer..")
      Thread.sleep(1000)
      println("finish produce")
      val r = {
        if(true) throw new Exception
        "sdf"
      }
      p success r
    }

    val consumer = {
      println("start consumer")
      f onSuccess {
        case r => println(f.value.get.get)
      }
    }
    println("stopping demo1")
  }

  def main(args: Array[String]) {
    demo1()
    Thread.sleep(5000)
    println("stopMainX")
  }
}
