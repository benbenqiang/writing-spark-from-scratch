package io.bio

import java.io._
import java.nio.CharBuffer

/**
  * 程序中数据的转出和保存都是用的流的方式。
  * java BIO 中有两类流，字符 和 字节 两个字节等于一个字符
  * 字节流相关： OutputStream InputStream
  * 字符流相关： Writer Reader
  *
  * FileWriter是通过OutputStreamWriter将字符转化成字节流,无论如何磁盘中保存的都是字节
  * 内存操作流: ByteArrayInputStream ByteArrayOutputStream
  * Created by bbq on 2016/6/1
  */
object BioBasic {
  val fileSep = File.separator
  val file = new File(s"scala-learning${fileSep}src${fileSep}main${fileSep}resources${fileSep}biobasic.data")

  /**字节流输出*/
  def fileOutput()={
    val fos = new FileOutputStream(file)
    fos.write("heheda".getBytes())
    fos.close()
  }
  /**字节流输入*/
  def fileInput()={
    val fis = new FileInputStream(file)
    val buffer = new Array[Byte](100)
    val n = fis.read(buffer)
    println(new String(buffer,0,n))
  }
  /**字符流输出*/
  def writerOutput()={
    val ow = new FileWriter(file)
    ow.write("heheda")
    ow.flush()
    ow.close()
  }
  /**字符流输入*/
  def readerInput()={
    val fr = new FileReader(file)
    val buffer = new Array[Char](10000)
    fr.read(buffer)
    printf(buffer.mkString("\n"))
  }

  /**将字节流转化为字符流*/
  def byte2charStream()={
    val fos = new FileOutputStream(file)
    val cos = new OutputStreamWriter(fos)
    cos.write("呵呵")
    cos.close()
  }


  /**数据输入输出都在内存中*/
  def memoryStream()={
    val bis = new ByteArrayInputStream("hehesd盖好盖好改好da".getBytes())
    val bos = new ByteArrayOutputStream()

    var temp =0
    def isContinue={
      temp = bis.read()
      temp != -1
    }
    while(isContinue){
      bos.write(temp)
    }
    bis.close()
    bos.close()
    println(bos.toString())
  }

  /**数据输入输出在内存中，将字节流转化为字符流进行操作,中间缓存用NIO中的CharBuffer*/
  def memoryStreamByChar()={
    val bis = new ByteArrayInputStream("hehesd盖好盖好改好da".getBytes())
    val bos = new ByteArrayOutputStream()

    val br = new InputStreamReader(bis)
    val bw = new OutputStreamWriter(bos)

    val buffer = CharBuffer.allocate(100)

    var charsRead = br.read(buffer)
    while(charsRead != -1){
      /**准备从buffer中读取数据*/
      buffer.flip()
      bw.write(buffer.array())
      /**准备往buffer中写数据*/
      buffer.clear()
      charsRead = br.read(buffer)
    }
    br.close()
    bw.close()
    println(bos.toString())
  }

  def main(args: Array[String]) {
    memoryStreamByChar()
  }

}
