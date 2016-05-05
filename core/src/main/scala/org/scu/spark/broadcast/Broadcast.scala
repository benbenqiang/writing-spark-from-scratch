package org.scu.spark.broadcast

import org.scu.spark.Logging

/**
 * 广播变量抽象类，将只读数据通过广播的方式传送到所有节点上。
 * Created by bbq on 2016/5/5
 */

abstract class Broadcast[T](val id:Long) extends Serializable with Logging{

}
