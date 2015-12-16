package concurrent

import java.util.concurrent.TimeUnit


import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
 * Future的使用方法
 * 1.非阻塞并行操作，占位符对象，指代某些尚未完成的计算结果。
 * 2.一般获取结果不采用阻塞方式，而是依赖回掉进行非阻塞操作。
 *
 * 相关学习地址：https://code.csdn.net/DOC_Scala/chinese_scala_offical_document/file/Futures-and-Promises-cn.md#anchor_0
 * Created by bbq on 2015/11/13
 */
object FutureDemo {
  //导入执行上下文，可以看作为线程池
 import scala.concurrent.ExecutionContext.Implicits.global

  /**
   * 1.开始一个异步调用
   * 2.使用阻塞的方式获取结果（不推荐）
   */
  def demo1()={
    //1.开始一个异步应用
    val f :Future[Int] = future{
      Thread.sleep(1000)
      1 + 1
    }
    val result = Await.result(f,Duration(3,TimeUnit.SECONDS))
    println(result)
    println("other code")
  }

  /**
   * 采用异步回调取回结果
   */
  def demo2()={
    val f :Future[Int] = future{
      Thread.sleep(1000)
      if(true) throw new Exception()
      1 + 1
    }
    f.onComplete{
      case Success(e) => println("success:" + e)
      case Failure(e) => println("failure:" + e)
    }
    Thread.sleep(2000)
    println("主线程运行完毕")
  }

  /**
   * Future 组合，Future作为一种类型容器，可以进行map，flatMap的操作，可以用在for语句中
   * map 与 flatmap的区别，map只要第一层future完成，就会调用最外层的回调，而flatmap会等到最后一个future运行的结果
   */
  def demo3()={
    val husband = getMarried("Bob")
    val father = husband.flatMap(makeBaby)
    father.onComplete {
      case Success(e) => println("becomes father successful")
      case Failure(e) => println("fail to be father:"+e)
    }
    println("异步调用，回到主线程")
    Thread.sleep(6000)
    println("主线程完成")
  }

  def main(args: Array[String]) {
    demo3()
  }

  case class Husband(name:String)
  case class Father(name:String)

  /**
   * 结婚成为丈夫
   * 返回Future
   */
  def getMarried(name:String):Future[Husband]=future{
    println("marring.....")
    Thread.sleep(1000)
//    if(true) throw new Exception("failure")
    println(name+"becomes a husband")
    Husband(name)
  }

  /**
   * 结婚后成为父亲
   */
  def makeBaby(husband: Husband):Future[Father]=future{
    println("loving...")
    Thread.sleep(1000)
    if(false){
      throw new Exception("failure")
    }else{
      println(husband.name+" becomes a father")
    }
    Father(husband.name)
  }

}
