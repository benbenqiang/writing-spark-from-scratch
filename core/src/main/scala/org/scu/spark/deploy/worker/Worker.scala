package org.scu.spark.deploy.worker

import java.io.{IOException, File}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{TimeUnit, Executors}

import akka.actor.{PoisonPill, Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.google.protobuf.RpcUtil
import org.scu.spark.{SparkConf, Logging}
import org.scu.spark.deploy.DeployMessage._
import org.scu.spark.deploy.master.Master
import org.scu.spark.rpc.akka.{AkkaRpcEnv, AkkaUtil, RpcAddress, RpcEnvConfig}
import org.scu.spark.util.{RpcUtils, Utils}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Created by bbq on 2015/11/11
 */
class Worker(
              rpcEnv: AkkaRpcEnv,
              cores: Int,
              memory: Int,
              masterRpcAddress:RpcAddress,
              systemName:String,
              endpointName:String,
              workDirpath : String = null,
              val conf:SparkConf
              ) extends Actor with Logging {
  //用于生成Woker的ID
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port
  private var master: Option[ActorRef] = None
  private var activeMasterUrl :String = ""
  private val workerId = generateWorkerId()
  private val HEARTBEAT_MILLIS= 30 * 1000

  val workerUri = AkkaUtil.generateRpcAddress(systemName,AkkaUtil.getRpcAddressFromSys(rpcEnv.actorSystem),endpointName)
  val executors = new mutable.HashMap[String,ExecutorRunner]

  private val sparkHome = new File(".")

  var workerDir :File = null
  val appDirectories = new mutable.HashMap[String,Seq[String]]

  var coresUsed = 0
  var memoryUsed = 0

  def coresFree :Int = cores  - coresUsed
  def memoryFree :Int = memory  - memoryUsed

  private def createWorkDir() = {
    workerDir = Option(workDirpath).map(new File(_)).getOrElse(new File(sparkHome,"worker"+workerId))
    try {
      workerDir.mkdir()
      if (!workerDir.exists() || !workerDir.isDirectory) {
        logError("Failed to create work directory" + workerDir)
        System.exit(1)
      }
      assert(workerDir.isDirectory)
    }catch {
      case e:Exception =>
        logError("Failed to create work directiory"+ workerDir)
        System.exit(1)
    }
  }

  override def preStart() = {
    logInfo(s"Starting Spark worker $host:$port with $cores cores,$memory RAM")
    logInfo(s"Running Spark version ${org.scu.spark.SPARK_VERSION}")
    createWorkDir()
    //向Master注册
    tryRegisterMasters()
  }

  override def receive: Receive = {
    case SendHeartbeat =>
      logDebug("Send hearbeat to master")
      master.get ! Heartbeat(workerId)

    case LaunchExecutor(masterUrl,appId,execId,appDesc,cores_,memory_) =>
      if(masterUrl != activeMasterUrl){
        logWarning(s"Invalid Master ($masterUrl) attempted to launch executor")
      }else{
        logInfo(s"Asked to launch executor $appId / $execId for $appId")

        /**为executor创建目录*/
        val exectutorDir = new File(workerDir,appId +"-" + execId)
        if (!exectutorDir.mkdir()){
          throw  new IOException("Failed to create directory"+exectutorDir)
        }

        //TODO appDirectoreos

        val manager = new ExecutorRunner(
        appId,
        execId,
        appDesc.copy(),
        cores_,
        memory_,
        self,
        workerId,
        host,
        publicAddress = host,
        sparkHome,
        exectutorDir,
        workerUri,
        conf,
        ExecutorState.RUNNING
        )
        executors(appId+"/"+execId) = manager
        /**真正启动Executor的地方*/
        manager.start()
        coresUsed += cores_
        memoryUsed += memory_
        /**先通知Master，再由master通知appclient*/
        master.get ! ExecutorStateChanged(appId,execId,manager.state,None,None)

      }
    case _ =>
      logError("no receive defined!")
  }

  /**
   * 链接远程Master的Actor
   */
  def tryRegisterMasters() = {
    logInfo("Connecting to master:" + masterRpcAddress)
    val masterEndpoint = rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, masterRpcAddress, Master.ACTOR_NAME)
    registerMaster(masterEndpoint)
  }

  /**
   * 链接成功后向Master注册Worker
   */
  def registerMaster(master: ActorRef) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    implicit val timeout = RpcUtils.askRpcTimeout(conf)
    val future = master ? RegisterWorker(workerId, host, port, cores, memory,self)
    //异步回调
    future.onComplete{
      case Success(response)=>
        handleRegisterResponse(response,master)
      case Failure(response)=>
        logError("Register to Master Error :"+response)
        System.exit(1)
    }

  }


  /**
   * 处理向Master注册后，Master返回的信息
   */
  def handleRegisterResponse(msg: Any,master:ActorRef) = {
    msg match {
      case RegisteredWorker() =>
        changeMaster(master)
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable {
          override def run(): Unit = self ! SendHeartbeat
        },0,HEARTBEAT_MILLIS,TimeUnit.MILLISECONDS)
      case RegisterWorkerFaild(str) =>
        logError("Worker registration failed: " + str)
    }
  }

  /**
   * 配置Worker的MasterActorRef
   */
  def changeMaster(masterRef: ActorRef) = {
    activeMasterUrl = AkkaUtil.getRpcAddressFromActor(masterRef).toSparkURL
    master = Some(masterRef)
  }

  /**
   * 根据时间和host：port 生成WokerID
   * @return
   */
  private def generateWorkerId(): String = {
    val date = createDateFormat.format(new Date)
    s"Worker-$date-$host-$port"
  }
}

object Worker extends Logging {
  val SYSTEM_NAME = "sparkWorker"
  val ACTOR_NAME = "Worker"

  def main(args: Array[String]) {
    val conf = new SparkConf()
    val masterHost = conf.get("spark.master.host")
    val masterPort = conf.getInt("spark.master.port")

    val rpcConfig = new RpcEnvConfig(SYSTEM_NAME, "127.0.0.1", 60002)
    val masterRpcAddress = RpcAddress(masterHost, masterPort)
    val rpcEnv = new AkkaRpcEnv(AkkaUtil.doCreateActorSystem(rpcConfig))
    val actorRef = rpcEnv.doCreateActor(Props(classOf[Worker], rpcEnv, 200, 102400, masterRpcAddress, SYSTEM_NAME, ACTOR_NAME, null, conf), ACTOR_NAME)
  }
}