package org.scu.spark.deploy.master

import java.util.Date

import akka.actor.ActorRef
import org.scu.spark.deploy.ApplicationDescription

/**
 * Created by bbq on 2015/12/17
 */
private[spark] class ApplicationInfo(
                                    val startTime:Long,
                                    val id:String,
                                    val desc:ApplicationDescription,
                                    val summitDate:Date,
                                    val driver:ActorRef,
                                    defaultCores:Int
                                      )extends Serializable{

}
