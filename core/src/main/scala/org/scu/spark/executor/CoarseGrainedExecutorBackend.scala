package org.scu.spark.executor

import java.net.URL
import java.nio.ByteBuffer

import akka.actor.{Props, Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import org.scu.spark.TaskState.TaskState
import org.scu.spark.rpc.akka.{AkkaUtil, RpcEnvConfig, AkkaRpcEnv}
import org.scu.spark.scheduler.cluster.CoarseGrainedClusterMessage._
import org.scu.spark.scheduler.cluster.TaskDescription
import org.scu.spark.util.{RpcUtils, ThreadUtils}
import org.scu.spark.{SparkConf, Logging, SparkEnv}

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import scala.concurrent.duration._

/**
 * Created by bbq on 2016/1/13
 */
private[spark] class CoarseGrainedExecutorBackend(
                                                   val rpcEnv: AkkaRpcEnv,
                                                   driverUrl: String,
                                                   executorId: String,
                                                   hostPort: String,
                                                   cores: Int,
                                                   userClassPath: Seq[URL],
                                                   env: SparkEnv
                                                   ) extends Actor with ExecutorBackend with Logging {
  var executor: Executor = null
  @volatile var driver: Option[ActorRef] = None

  private[this] val ser = env.closureSerializer.newInstance()

  override def preStart(): Unit = {
    logInfo("Connecting to driver:"+ driverUrl)
    /**future执行环境*/
    implicit val context = ThreadUtils.sameThread
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap{ ref => {
      driver = Some(ref)
      implicit val timeout = Timeout(100 seconds)
      (ref ? RegisterExecutor(executorId,self,cores,extractLogUrls)).mapTo[RegisterExecutorResponse]
    }}.onComplete{
      case success:Success[RegisterExecutorResponse]=>
        logInfo("ExecutorBackend connected to DriverBackend")
        self ! success.get
      case Failure(e) =>
        logError(s"Cannot register with driver: $driverUrl",e)
        System.exit(1)
    }
  }

  def extractLogUrls:Map[String,String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filterKeys(_.startsWith(prefix))
    .map(e=> (e._1.substring(prefix.length).toLowerCase,e._2))
  }

  override def receive: Receive = {
    case RegisteredExectutor(hostname)=>
      logInfo("Successfully registered with driver")
      executor = new Executor(executorId,hostname,env,userClassPath,isLocal = false)
    case LaunchTask(data)=>
      if(executor == null){
        logError("Received LaunchTask command but executor was null")
        System.exit(1)
      }else{
        val taskDesc = ser.deserialize[TaskDescription](data.value)
        logInfo("Got assigned task"+taskDesc.taskId)
        executor.launchTask(this,taskDesc.taskId,taskDesc.attemptNumber,taskDesc.name,taskDesc.serializedTask)
      }

    case _ => logInfo("receive something")
  }

  /**ExecutorBackend将Executor 以及 Task的状态信息反馈给Driver*/
  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit = {
    val msg = StatusUpdate(executorId,taskId,state,data)
  }
}

private[spark] object CoarseGrainedExecutorBackend extends Logging{

  private def run(
                 driverUrl:String,
                 executorId:String,
                 hostname:String,
                 cores:Int,
                 appId:String,
                 workerUrl:Option[String],
                 userClassPath:Seq[URL]
                   ): Unit ={
    /**从driver获取sparkconf的配置*/
    val executorConf = new SparkConf()
    /**executor rpc端口*/
    val port = executorConf.getInt("spark.executor.port",0)
    assert(port.toInt != 0)
    /**fethcer的RPCEnv*/
    val rpcConfig = new RpcEnvConfig("driverPropsFethcer",hostname,port)
    val fetcher = new AkkaRpcEnv(AkkaUtil.doCreateActorSystem(rpcConfig))
    val driver: ActorRef = fetcher.setupEndpointRefByURI(driverUrl)
    val props = fetcher.askSyn[Seq[(String,String)]](driver,RetrieveSparkProps,executorConf) ++ Seq(("spark.app.id",appId))
    fetcher.actorSystem.shutdown()

    logInfo("finishConfFething")

    val driverConf = new SparkConf()
    for((key,value) <- props){
      driverConf.set(key,value)
    }

    val env = SparkEnv.createExecutorEnv(driverConf,executorId,hostname,port,cores,isLocal = false)

    val sparkHostPort = hostname + ":" + port
    env.rpcEnv.doCreateActor(Props(classOf[CoarseGrainedExecutorBackend],env.rpcEnv,driverUrl,executorId,sparkHostPort,cores,userClassPath,env),"Executor")

    env.rpcEnv.actorSystem.awaitTermination()
  }
  def main(args: Array[String]): Unit = {

    val argv = args.mkString(",").split("--").tail.map(_.split(",")).map(x=>(x(0),x(1))).toMap

    val driverUrl :String= argv.getOrElse("driver-url","akka.tcp://sparkDriver@127.0.0.1:60010/user/CoraseGrainedScheduler")
    val executorId : String = argv.getOrElse("executor-id","0")
    val hostname :String = argv.getOrElse("hostname","127.0.0.1")
    val cores : Int = argv.getOrElse("cores","2").toInt
    val appId :String = argv.getOrElse("app-id","app-test-id")
    val workerUrl :Option[String] = Some(argv.getOrElse("worker-url","akka.tcp://sparkWorker@127.0.0.1:60002/user/Worker"))
    val userClassPath = new mutable.ListBuffer[URL]()

    logInfo("Start Runing")
    run(driverUrl,executorId,hostname,cores,appId,workerUrl,userClassPath)
  }
}