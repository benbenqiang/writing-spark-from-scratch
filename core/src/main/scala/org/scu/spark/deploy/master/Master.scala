package org.scu.spark.deploy.master

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef, Props}
import org.scu.spark.deploy.ApplicationDescription
import org.scu.spark.deploy.DeployMessage._
import org.scu.spark.rpc.akka.{AkkaRpcEnv, AkkaUtil, RpcAddress, RpcEnvConfig}
import org.scu.spark.{Logging, SparkConf}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * spark集群的Master节点，主要职责：
 * 1.收取worker节点的心跳信息
 * 2.汇总集群资源情况
 * Created by bbq on 2015/11/10
 */
private[deploy] class Master(
                              akkaRpcEnv: AkkaRpcEnv,
                              address: RpcAddress,
                              val conf: SparkConf
                              ) extends Actor with Logging {
  val workers = new mutable.HashSet[WorkerInfo]()

  /**根据workerid和rpcAddres来定位worker*/
  private val idToWorker = new mutable.HashMap[String, WorkerInfo]
  private val addressToWorker = new mutable.HashMap[RpcAddress,WorkerInfo]()

  /**定位App*/
  val idToApp = new mutable.HashMap[String,ApplicationInfo]()
  private val endpointToApp = new mutable.HashMap[ActorRef,ApplicationInfo]()
  private val addressToApp = new mutable.HashMap[RpcAddress,ApplicationInfo]()

  /**维护App状态*/
  val apps = new mutable.HashSet[ApplicationInfo]()
  val waitingApps = new ArrayBuffer[ApplicationInfo]()
  private val completedApps = new ArrayBuffer[ApplicationInfo]()

  private val nextAppNumber = new AtomicInteger(0)
  val createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  private val masterUrl = address.toSparkURL

  /**应用占用最大的Cores个数，默认Int最大*/
  private val defaultCores = conf.getInt("spark.deploy.defaultCores",Int.MaxValue)

  override def preStart() = {
    logInfo("Starting Spark Master at" + masterUrl)
    logInfo(s"Running Spark version ${org.scu.spark.SPARK_VERSION}")

  }

  override def receive: Receive = {
    //收到Worker的注册信息
    case RegisterWorker(id, host, port, cores, memory) =>
      logInfo(s"Registering worker $id with $cores cores, $memory RAM")
      if(idToWorker.contains(id)){
        sender() ! RegisterWorkerFaild("Duplicate worker ID")
      }else{
        val worker = new WorkerInfo(id, host, port, cores, memory, sender())
        if(registerWorker(worker)){
          //TODO 持久化
          sender() ! RegisteredWorker()
          schedule()
        }else{
          val workerAddress = worker.workerAddress
          val warning = "worker registeration failed.Attemped to re-register worker at same address:"+workerAddress
          logWarning(warning)
          sender() ! RegisterWorkerFaild(warning)
        }
      }

    case Heartbeat(workerId) =>
      idToWorker.get(workerId) match {
        case Some(workInfo) =>
          workInfo._lastHeartbeat = System.currentTimeMillis()
          logDebug(s"Receiving HeartBeet from $workerId")
        case None =>
          logError(s"Got heartbeat from unregistered worker $workerId")
      }
    case RegisterApplication(description) =>
      println("Registering app" + description.name)
      val app = createApplication(description,sender())
      registerApplication(app)
      logInfo("Registerd app"+description.name+ " with ID "+app.id)
      //TODO PersisteneEngine 用于容灾恢复
      sender() ! RegisteredApplication(app.id,self)
      schedule()
  }
/**
   *  调度当前可用的资源。当新添加一个app或者资源变化时调用该方法
   */
  private def schedule():Unit={
    //TODO recoverStage judge
    val shuffledWorkers: mutable.HashSet[WorkerInfo] = Random.shuffle(workers)
//    for (worker <- shuffledWorkers if worker._state == WorkerState.ALIVE){
//
//    }
    /**根据application启动worker上的executor*/

  }

  //与Application相关方法

  /**根据创建时间，driveer的actorRef，创建application信息*/
  private def createApplication(desc: ApplicationDescription, driver: ActorRef): ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val appId = newApplicationId(date)
    new ApplicationInfo(now,appId,desc,date,driver,defaultCores)
  }

  /**获取Application ID */
  private def newApplicationId(submitDate:Date):String = {
    "app-%s-%04d".format(createDateFormat.format(submitDate),nextAppNumber.getAndIncrement())
  }

  private def registerApplication(app:ApplicationInfo):Unit={
    val appAddress = AkkaUtil.getRpcAddressFromActor(app.driver)
    if(addressToApp.contains(appAddress)){
      logInfo("Attempted to re-register applicaiton at same address:"+appAddress)
      return
    }
    //TODO applicationMetircSystem
    apps += app
    idToApp(app.id) = app
    endpointToApp(app.driver)=app
    addressToApp(appAddress) = app
    waitingApps += app
  }

  //与Worker相关方法

  private def launchExecutor(worker:WorkerInfo,exec:ExecutorDesc)={
    logInfo("Launching executor"+ exec.fullId + "on worker" + worker.id)
    worker.addExecutor(exec)
    /**通知worker初始化executor*/
    worker.endpoint ! LaunchExecutor(masterUrl,exec.application.id,exec.id,exec.application.desc,exec.cores,exec.memory)
    /**通知Application Driver Executor已经注册好了*/
    exec.application.driver ! ExecutorAdded(exec.id,worker.id,worker.hostPort,exec.cores,exec.memory)
  }

  /**接受来自Worker的信息*/
  private def registerWorker(worker:WorkerInfo):Boolean={
    /**过滤掉同一结点之前已经死了的worker*/
    workers.filter{w=>
      (w.host == worker.host && w.port == worker.port) && w._state ==WorkerState.DEAD
    }.foreach(w=>
    workers -= w
    )

    /**检测是否在同一哥host：port启动了两个worker,若旧worker状态不正常则代替*/
    val workerAddress = worker.workerAddress
    if (addressToWorker.contains(workerAddress)){
      val oldWorker = addressToWorker(workerAddress)
      removeWorker(oldWorker)
    }else{
      logInfo("Attempted to re-regeister worker at same address"+workerAddress)
      return false
    }
    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress)=worker
    true
  }

  private def removeWorker(worker:WorkerInfo): Unit ={
    logInfo("Removing worker"+worker.id+"on"+worker.host +":"+worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.workerAddress
    //TODO 向worker对应的appclient发送executor丢失的信息
    //TODO 向worker中的driver进行移除
    //TODO 持久化文件中删除worker
  }

  private def startExecutorsOnWorkers():Unit={
    for(app <- waitingApps if app.coresLeft > 0){
      val coresPerExecutor : Option[Int] = app.desc.coresPerExecutor
      //过滤出可以满足executor的worker,根据剩余的cores大小由高到低排列
      val usableWorkers = workers.toArray.filter(_._state==WorkerState.ALIVE)
      .filter(worker=>worker.memoryFree >= app.desc.memoryPerExecutorMB &&
      worker.coresFree >= coresPerExecutor.getOrElse(1)).sortBy(_.coresFree).reverse

      //TODO sheduleExecutorsOnWorkers

      for(pos <- usableWorkers.indices){
        allocateWorkerResourceToExecutors(app,coresPerExecutor.get,coresPerExecutor,usableWorkers(pos))
      }

    }
  }

  /**
    *
    */
  private def allocateWorkerResourceToExecutors(
                                               app:ApplicationInfo,
                                               assignedCores:Int,
                                               coresPerExecutor:Option[Int],
                                               worker:WorkerInfo
                                                 )={
    /**每个worker分配一个executor*/
    val exec = app.addExecutor(worker,assignedCores)


  }
}

object Master extends Logging {
  val SYSTEM_NAME = "sparkMaster"
  val ACTOR_NAME = "Master"

  def main(args: Array[String]) {
    val conf = new SparkConf()
    val masterHost = conf.get("spark.master.host")
    val masterPort = conf.getInt("spark.master.port")
    val rpcConfig = new RpcEnvConfig(SYSTEM_NAME, masterHost, masterPort)
    val rpcEnv = new AkkaRpcEnv(AkkaUtil.doCreateActorSystem(rpcConfig))

    val actorRef = rpcEnv.doCreateActor(Props(new Master(rpcEnv, RpcAddress(masterHost, masterPort), conf)), ACTOR_NAME)
  }
}