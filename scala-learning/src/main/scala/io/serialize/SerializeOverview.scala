package io.serialize

import java.io.{File, FileOutputStream, ObjectOutputStream}
import java.nio.ByteBuffer

/**
 * io.ObjectOutputStream 代表对象输出流，writeObject(obj)进行序列化
 * io.ObjectInputStream 代表从一个源输入流中读取字节序列。
 *
 * 只有实现了Serializable 和 externalizable 的接口的对象才能被序列化
 * Externalizable 由自身控制序列化行为
 * Serializable采用默认的序列化方式
 *
 * Created by bbq on 2016/4/7
 */
object SerializeOverview {
  def standerdInputOutput ={
    val buffer = ByteBuffer.allocate(10)
    buffer.put("wzq".getBytes)
    val task = new TaskDescription(10,"task",buffer)

    val fileSep = File.separator
    val oo = new ObjectOutputStream(new FileOutputStream(new File(s"scala-learning${fileSep}src${fileSep}main${fileSep}resources${fileSep}SerializeOverview.data")))
    oo.writeObject(task)
    println("序列化成功")
    oo.close()
  }

  def main(args: Array[String]) {
    standerdInputOutput
  }
}

private class TaskDescription(
                             val taskId:Long,
                             val name:String,
                             /**虽然这个不可序列化，但是并不作为对象的字段，因为没有加var或val，也没有被def升格*/
                             _serializedTask:ByteBuffer
                               ) extends Serializable{
  private val buffervalue: String = ""+_serializedTask
}