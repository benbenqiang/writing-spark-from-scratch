package org.scu.spark.scheduler

import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.atomic.AtomicBoolean

import org.scu.spark.Logging

/**
 * 维护一个阻塞队列，将事件post给队列。
 * 创建一个进程从队列中读消息。
 * Created by bbq on 2015/11/20
 */
private abstract class EventLoop[E](name:String) extends Logging{
  //阻塞消息队列
  private val eventQueue = new LinkedBlockingDeque[E]()

  private val stopped = new AtomicBoolean(false)

  //线程以阻塞方式读取数据
  private val enventThread = new Thread(name){
    setDaemon(true)

    override def run(): Unit = {
      while(!stopped.get()){
        val event = eventQueue.take()
        onReceive(event)
      }
    }
  }

  def start():Unit={
    onStart()
    enventThread.start()
  }

  def post(event:E): Unit ={
    eventQueue.put(event)
  }

  protected def onStart():Unit={}

  protected def onReceive(event:E):Unit

  protected def onError(e:Throwable):Unit
}
