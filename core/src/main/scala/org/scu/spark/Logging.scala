package org.scu.spark

import org.apache.log4j.Logger

/**
 * 打日志工具类
 * Created by applelab on 2015/11/11
 */
trait Logging {
  private  val log  : Logger = Logger.getLogger(this.getClass.getName.stripSuffix("$"))

  def logDebug(msg:String)={
    log.debug(msg)
  }

  def logInfo(msg:String)={
    log.info(msg)
  }

  def logWarning(msg:String)={
    log.warn(msg)
  }

  def logError(msg:String)={
    log.error(msg)
  }

  def logFatal(msg:String)={
    log.fatal()
  }

  def logWarning(msg:String,throwable: Throwable)={
    log.fatal(msg,throwable)
  }
}
