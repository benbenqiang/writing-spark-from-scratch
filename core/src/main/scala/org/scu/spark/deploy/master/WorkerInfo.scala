package org.scu.spark.deploy.master

import akka.actor.ActorRef
import org.scu.spark.rpc.akka.RpcAddress

import scala.collection.mutable

/**
 * Created by bbq on 2015/11/12
 */
class WorkerInfo(
                val id :String,
                val host:String,
                val port:Int,
                val cores:Int,
                val memory:Int,
                val endpoint:ActorRef
                  ){

  @transient var _executros:collection.mutable.HashMap[String,ExecutorDesc]=_
  @transient var _coresUsed : Int = _
  @transient var _memoryUsed :Int = _

  @transient var _lastHeartbeat : Long = _

  var _state :WorkerState.Value = _

  val workerAddress = RpcAddress(host,port)

  init()

  def memoryFree : Int = memory - _memoryUsed
  def coresFree :Int = cores - _coresUsed

  private def init(): Unit ={
    _state = WorkerState.ALIVE
    _lastHeartbeat = System.currentTimeMillis()
    _executros = new mutable.HashMap[String,ExecutorDesc]
  }

  def hostPort :String = {
    assert(port>0)
    host + ":" + port
  }
  def setState(state:WorkerState.Value) = {
    this._state = state
  }
  
  def addExecutor(exec:ExecutorDesc): Unit ={
    _executros(exec.fullId)=exec
    _coresUsed += exec.cores
    _memoryUsed += exec.memory
  }

}