package org.scu.spark.launcher

import java.io.File
import java.util

import org.scu.spark.deploy.Command
import scala.collection.JavaConverters._
/**
 * 此类用于生成在Worker中运行进程的processBuilder
 * 之所以单独将此类放在这个包明下是因为，该类要继承AbstractCommandBuilder，这是一个私有的包api，因为java中
 * 没有像scala一样可以指定private[spark]的方法，所以只能将这两个类放在同一个包路径下
 * Created by bbq on 2016/1/8
 */
private[spark] class WorkerCommandBuilder(sparkHome:String,memoryMb:Int,command:Command) extends AbstractCommandBuilder{

  childEnv.putAll(command.environment.asJava)
  childEnv.put(CommandBuilderUtils.ENV_SPARK_HOME,sparkHome)

  override def buildCommand(env: util.Map[String, String]): util.List[String] = {
    val cmd = buildJavaCommand(command.classPathEntries.mkString(File.pathSeparator))
    cmd.add(s"-Xms${memoryMb}M")
    cmd.add(s"-Xmx${memoryMb}M")
    command.javaOpts.foreach(cmd.add)
    CommandBuilderUtils.addPermGenSizeOpt(cmd)
    addOptionString(cmd,getenv("SPARK_JAVA_OPTS"))
    cmd
  }

  def buildCommand():util.List[String]= buildCommand(new util.HashMap[String,String])

}
