package org.scu.spark

import akka.actor.{Actor, Props, ActorRef, ActorSystem}
import org.scu.spark.broadcast.BroadcastManager
import org.scu.spark.deploy.master.Master
import org.scu.spark.rpc.akka.{AkkaUtil, AkkaRpcEnv, RpcEnvConfig}
import org.scu.spark.serializer.{JavaSerializer, Serializer}
import org.scu.spark.storage.{BlockManagerMasterEndpoint, BlockManagerMaster}
import org.scu.spark.util.{RpcUtils, Utils}

/**
 * 保存运行时对象，例如rpcEnv,bloclManager,output tracker等
 * Created by bbe on 2015/12/11
 */
class SparkEnv (
               val executorId:String,
               private[spark] val rpcEnv:AkkaRpcEnv,
               val serializer: Serializer,
               val closureSerializer:Serializer,
               val broadcastManager : BroadcastManager,
               val conf : SparkConf
                 )extends Logging{
  private[spark] var isStopped = false

  private[spark] def stop(): Unit ={
    if(!isStopped){
      isStopped = true
      rpcEnv.actorSystem.shutdown()
    }
  }
}

object SparkEnv extends Logging{
  private var _env : SparkEnv = _

  private[spark] val driverActorSystemName = "sparkDriver"
  private[spark] val executorActorSystemName = "sparkExecutor"

  def env = _env
  def env_=(e:SparkEnv)={
    _env = e
  }

  /**
   * Driver的SparkEnv，包含RPCEnv
   */
  private[spark] def createDriverEnv(
                                    conf:SparkConf,
                                    isLocal:Boolean,
                                    numCores:Int
                                      ): SparkEnv ={
    val hostname = conf.get("spark.driver.host")
    val port = conf.getInt("spark.driver.port")
    create(conf,SparkContext.DRIVER_IDENTIFIER,hostname,port,isDriver = true,isLocal,numCores)
  }

  /**
   * Executor的SparkEnv，包含RPCEnv
   */
  private[spark] def createExecutorEnv(
                                      conf:SparkConf,
                                      executorId:String,
                                      hostname:String,
                                      port:Int,
                                      numCores:Int,
                                      isLocal:Boolean
                                        ):SparkEnv={
    val env = create(conf,executorId,hostname,port,isDriver = false,isLocal,numCores)
    SparkEnv._env = env
    env
  }

  private def create (
                     conf:SparkConf,
                     executorId:String,
                     hostname:String,
                     port:Int,
                     isDriver:Boolean,
                     isLocal:Boolean,
                     numUsableCores:Int
                       ):SparkEnv={
    val actorSystemName = if (isDriver) driverActorSystemName else executorActorSystemName
    val rpcConfig  = new RpcEnvConfig(actorSystemName,hostname,port)
    val actorSystem = AkkaUtil.doCreateActorSystem(rpcConfig)
    val rpcEnv = new AkkaRpcEnv(actorSystem)

    /**初始化给定类名称的类*/
    def instantiateClass[T](className:String):T={
      val cls = Utils.classForName(className)
      /**先寻找有没有以SparkConf 和 isDriver为参数的狗仔函数，如果没有，就寻找仅以sparkConf的，在没有就调用无参构造函数*/
      try{
        cls.getConstructor(classOf[SparkConf],Boolean.getClass).newInstance(conf,isDriver).asInstanceOf[T]
      }catch{
        case _ : NoSuchMethodException =>
          try{
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _ :NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    /**从SparkConf中获取类名，并初始化，例如 spark.serializer -> org.apache.spark.serializer.JavaSerializer*/
    def instantiateClassFromConf[T](propertyName:String,defaultClassName:String):T={
      instantiateClass[T](conf.get(propertyName,defaultClassName))
    }

    val serializer = instantiateClassFromConf[Serializer]("spark.serializer","org.apache.spark.serializer.JavaSerializer")

    val closureSerializer = new JavaSerializer(conf)

    val broadcastManager = new BroadcastManager(isDriver,conf)

    /**如果是driver端的sparkEnv实例化，那么创建对象，如果是executor的就用远程连接*/
    def registerOrLoopupEndpoint(name:String,endpointCreator: => Actor):ActorRef={
      if(isDriver){
        logInfo("Create Actor : "+ name)
        rpcEnv.doCreateActor(Props(endpointCreator),name)
      } else {
        RpcUtils.makeDriverRef(name,conf,rpcEnv)
      }
    }

    val blockManagerMaster = new BlockManagerMaster(registerOrLoopupEndpoint(BlockManagerMaster.DRIVER_ENDPOINT_NAME,new BlockManagerMasterEndpoint(rpcEnv,isLocal,conf)),conf,isDriver)

    val envInstance = new SparkEnv(executorId,rpcEnv,serializer,closureSerializer,broadcastManager,conf)
    envInstance
  }

}
