package org.scu.spark.scheduler.cluster

import java.util.concurrent.Semaphore

import org.scu.spark.deploy.ApplicationDescription
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

  private var appId:String = _

  private val maxCores = conf.getOption("spark.cores.max").map(_.toInt)

  private val registrationBarrier = new Semaphore(0)

  override def start(): Unit = {
    super.start()
    //TODO launcherBackend

    /** executor 远程连接Driver 的地址 */
    val driverUrl = AkkaUtil.generateRpcAddress(
      SparkEnv.driverActorSystemName, sc.conf.get("spark.driver.host"),
      sc.conf.getInt("spark.driver.port"), CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
    val appDesc = ApplicationDescription(sc.appName,maxCores,sc.executorMemory)
    _client = new AppClient(sc.env.rpcEnv, masters,appDesc, this, conf)
    _client.start()
    //TODO setApp state using launcherBackend
    waitForRegistration()
  }

  /**
    * appclient 向 master成功注册了应用
    */
  override def connected(appId: String): Unit = {
    logInfo("connected to spark cluster with app Id "+appId)
    this.appId = appId
    notifyContext()
    //TODO launcherBackend set AppId
  }

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

  /**当client向Master 注册application信息的时候，使用该方法阻塞等待*/
  private def waitForRegistration()={
    registrationBarrier.acquire()
  }

  /**当client成功注册applicaiton后，调用该方法释放阻塞的进程*/
  private def notifyContext()={
    registrationBarrier.release()
  }
}
