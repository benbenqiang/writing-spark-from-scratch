package org.scu.spark.deploy

/**
 * 起一个进程运行一个class，需要输入参数，环境变量，classpath，lib，jvm配置等
 * classPathEntries 为了系统兼容性而存在，功能与libraryPathEntries一样
 * Created by bbq on 2015/12/26
 */
private[spark] case class Command(
                                   mainClass: String,
                                   arguments: Seq[String],
                                   environment: collection.Map[String, String],
                                   classPathEntries: Seq[String],
                                   libraryPathEntries: Seq[String],
                                   javaOpts: Seq[String]
                                   )
