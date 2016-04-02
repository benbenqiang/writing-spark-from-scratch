package org.scu.spark.rdd

/**
 * Created by bbq on 2016/4/1
 */
class SparkException(message:String,cause:Throwable)
  extends Exception(message,cause) {
  def this(message:String) = this(message,null)
}
