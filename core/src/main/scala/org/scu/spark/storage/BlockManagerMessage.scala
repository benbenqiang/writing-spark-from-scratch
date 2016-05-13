package org.scu.spark.storage

import java.io.{ObjectInput, ObjectOutput, Externalizable}

/**
 * Created by bbq on 2016/5/13
 */
private[spark] object BlockManagerMessage {
  /**Messages from the master to slaves*/


  /**Message from slaves to the master*/
  sealed trait ToBlockManagerMaster

  case class UpdateBlockInfo(
                             var blockManagerId:BlockManagerId,
                             var blockId:BlockId,
                             var storageLevel: StorageLevel,
                             var memSize:Long,
                             var diskSize:Long) extends ToBlockManagerMaster
}
