package org.scu.spark.storage

import org.scu.spark.SparkConf
import org.scu.spark.rpc.akka.AkkaRpcEnv

/**
 * Driver 和Executor 端都有这个类，负责对block的存储和读取（远程或者本地）。
 * Created by bbq on 2016/5/9
 */
private[spark] class BlockManager(
                                 executorId:String,
                                 rpcEnv:AkkaRpcEnv,
                                 val master : BlockManagerMaster,
                                 val conf : SparkConf,
                                 numUsableCores:Int
                                   ) extends {

}
