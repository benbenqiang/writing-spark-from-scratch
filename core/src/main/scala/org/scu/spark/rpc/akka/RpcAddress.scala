package org.scu.spark.rpc.akka

/**
 * Created by bbq on 2015/11/11
 */
private[spark] case  class RpcAddress(host:String,port:Int){
  def toSparkURL :String = "spark://"+host+":"+port
}
