package org.scu.spark.executor

import java.net.URL

import org.scu.spark.SparkEnv

/**
 * Created by bbq on 2016/1/13
 */
class Executor(
              executorId:String,
              executorHostname:String,
              env:SparkEnv,
              userClassPath:Seq[URL]=Nil,
              isLocal:Boolean=false
                ) {

}
