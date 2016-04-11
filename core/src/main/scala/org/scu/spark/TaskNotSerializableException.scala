package org.scu.spark

/**
 * Created by bbq on 2016/4/11
 */
private[spark] class TaskNotSerializableException(error:Throwable) extends Exception(error)
