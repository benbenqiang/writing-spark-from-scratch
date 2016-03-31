package org.scu.spark.util.logging

import java.io.{FileOutputStream, File, InputStream}

import org.scu.spark.{SparkConf, Logging}

/**
 * 从一个输入流持续的数据持续的写入文件
 * Created by bbq on 2016/2/8
 */
private[spark] class FileAppender(inputStream:InputStream,file:File,bufferSize:Int=8192) extends Logging{
  @volatile private var outputStream:FileOutputStream = null
  @volatile private var markedForStop=false

  /**从stream中读取数据并且写入文件的线程*/
  private val writingThread = new Thread("File appending thread for "+ file){
    setDaemon(true)
    override def run(): Unit ={
      appendStreamToFile()
    }
  }
  writingThread.start()

  /**等待些县城结束*/
  def awaitTermination(): Unit ={
    writingThread.join()
  }

  def stop(): Unit ={
    markedForStop = true
  }

  protected def appendStreamToFile(): Unit ={
    try {
      logDebug("Started appending thread")
      openFile()
      val buf = new Array[Byte](bufferSize)
      var n = 0
      while(!markedForStop && n != -1){
        n = inputStream.read(buf)
        if (n > 0){
          appendToFile(buf,n)
        }
      }
    }catch {
      case e:Exception =>
        logError(s"Error writing stream to file $file" ,e)
    }finally {
      closeFile()
    }
  }

  protected def appendToFile(bytes:Array[Byte],len:Int): Unit ={
    if(outputStream == null){
      openFile()
    }
    outputStream.write(bytes,0,len)
  }


  /**打开文件输出流*/
  protected def openFile(): Unit ={
    outputStream = new FileOutputStream(file,false)
    logDebug(s"Opened file $file")
  }

  protected def closeFile(): Unit ={
    outputStream.flush()
    outputStream.close()
    logDebug(s"Closed file $file")
  }

}

private[spark] object FileAppender extends Logging{
  def apply(inputStream:InputStream,file:File,conf:SparkConf):FileAppender={
    new FileAppender(inputStream,file)
  }
}
