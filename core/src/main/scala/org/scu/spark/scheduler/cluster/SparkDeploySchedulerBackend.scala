package org.scu.spark.scheduler.cluster

import org.scu.spark.rpc.akka.{AkkaUtil, RpcAddress}
import org.scu.spark.scheduler.TaskSchedulerImpl
import org.scu.spark.scheduler.client.{AppClient, AppClientListener}
import org.scu.spark.{Logging, SparkContext, SparkEnv}

/**
 * Spark Standalone 用于分配资源。
 * Created by bbq on 2015/12/11
 */
class SparkDeploySchedulerBackend(
                                   scheduler: TaskSchedulerImpl,
                                   sc: SparkContext,
                                   masters: RpcAddress
                                   )
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
  with AppClientListener
  with Logging {
  private var _client: AppClient = _


  override def start(): Unit = {
    super.start()
    //TODO launcherBackend

    /** executor 远程连接Driver 的地址 */
    val driverUrl = AkkaUtil.address(
      SparkEnv.driverActorSystemName, sc.conf.get("spark.driver.host"),
      sc.conf.getInt("spark.driver.port"), CoarseGrainedSchedulerBackend.ENDPOINT_NAME)

    _client = new AppClient(sc.env.rpcEnv, masters, this, conf)
    _client.start()
  }


  override def connected(appId: String): Unit = ???

  /**
   * disconnection是一个短暂的状态，我们会恢复到一个新的Master上
   */
  override def disconnected(): Unit = ???

  override def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int, memory: Int): Unit = ???

  override def executorRemoved(fullId: String, message: String, exitStatus: Option[Int]): Unit = ???

  /**
   * 当一个应用发生了不可恢复的错误是调用
   */
  override def dead(reason: String): Unit = ???
}
