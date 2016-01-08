package org.scu.spark.deploy.worker

import java.io.File

import scala.collection.JavaConverters._

import org.scu.spark.Logging
import org.scu.spark.deploy.Command
import org.scu.spark.launcher.WorkerCommandBuilder
import org.scu.spark.util.Utils

/**
 * 运行command的工具类
 * Created by bbq on 2015/12/27
 */
private[deploy] object CommandUtils extends Logging {

  /**
   * 根据command,其他参数，以及classpath等，创建进程
   */
  def buildProcessBuilder(
                           command: Command,
                           //TODO SecurityManager
                           memory: Int,
                           sparkHome: String,
                           substituteArguments: String => String,
                           classPaths: Seq[String] = Nil,
                           env: Map[String, String] = sys.env
                           ): ProcessBuilder = {
    val localCommand  = buildLocalCommand(command,substituteArguments,classPaths,env)
    val commandSeq = buildCommandSeq(localCommand,memory,sparkHome)
    val builder = new ProcessBuilder(commandSeq:_*)
    val environment = builder.environment()
    for ((key,value) <- localCommand.environment){
      environment.put(key,value)
    }
    builder
  }

  /**将command转化为Seq，提供给ProcessBuilder运行*/
  private def buildCommandSeq(command:Command,memory:Int,sparkHome:String):Seq[String]={
    val cmd = new WorkerCommandBuilder(sparkHome,memory,command).buildCommand()
    cmd.asScala ++ Seq(command.mainClass) ++ command.arguments
  }
  /**
   * 根据系统更新Command，以及extra class path
   * */
  private def buildLocalCommand(
                                 command: Command,
                                 substituteArguments: String => String,
                                 classPath: Seq[String] = Nil,
                                 env: Map[String, String]
                                 ): Command = {
    /** 环境变量的名称 */
    val libraryPathName = Utils.libraryPathName
    /** 启动ExecutorJVM的参数 */
    val libraryPathEntries = command.libraryPathEntries
    /**相应系统的cmd环境变量*/
    val cmdLibraryPath = command.environment.get(libraryPathName)

    /**加入相应系统的环境变量*/
    val newEnvironment = if (libraryPathEntries.nonEmpty && libraryPathName.nonEmpty) {
      val libraryPaths = libraryPathEntries ++ cmdLibraryPath ++ env.get(libraryPathName)
      command.environment + ((libraryPathName,libraryPaths.mkString(File.pathSeparator)))
    } else {
      command.environment
    }

    Command(
    command.mainClass,
    command.arguments.map(substituteArguments),
    newEnvironment,
    command.classPathEntries ++ classPath,
    Nil,
    command.javaOpts
    )
  }
}
