package org.scu.spark.util

/**
 * Created by bbq on 2015/12/23
 */
object Utils {
  def checkHostPort(hostPort:String,message:String=""): Unit ={
    assert(hostPort.indexOf(":") != -1,message)
  }
}
