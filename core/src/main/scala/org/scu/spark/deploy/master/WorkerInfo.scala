package org.scu.spark.deploy.master

import akka.actor.ActorRef
import org.scu.spark.rpc.akka.RpcAddress

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

  var _lastHeartbeat : Long = _

  var _state :WorkerState.Value = _

  val workerAddress = RpcAddress(host,port)

  init()

  private def init(): Unit ={
    _state = WorkerState.ALIVE
    _lastHeartbeat = System.currentTimeMillis()
  }

  def setState(state:WorkerState.Value) = {
    this._state = state
  }

}