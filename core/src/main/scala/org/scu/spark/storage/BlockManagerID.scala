package org.scu.spark.storage

import java.io.{ObjectInput, ObjectOutput, Externalizable}

/**
 * 唯一代表一个BlockManager
 * Created by bbq on 2015/11/26
 */
class BlockManagerID(
                      private var executorID_ : String,
                      private var host_ : String,
                      private var port_ : Int
                      ) extends Externalizable{


  override def readExternal(in: ObjectInput): Unit = {

  }

  override def writeExternal(out: ObjectOutput): Unit = {

  }
}
