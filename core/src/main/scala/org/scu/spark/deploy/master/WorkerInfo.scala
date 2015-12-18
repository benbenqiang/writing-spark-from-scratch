package org.scu.spark.deploy.master

import akka.actor.ActorRef

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

  var lastHeartbeat : Long = _


}