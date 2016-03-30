package org.scu.spark.util

import java.io.{ByteArrayInputStream, File}
import java.nio.charset.StandardCharsets

import com.google.common.io.Files
import org.scalatest.BeforeAndAfter
import org.scu.spark.SparkFunSuite
import org.scu.spark.util.logging.FileAppender

/**
 * Created by bbq on 2016/3/30
 */
class FIleAppenderSuite extends SparkFunSuite with BeforeAndAfter{

  val testFile= new File(Utils.createTempDir(),"FileAppenderSuite-test").getAbsoluteFile

  before{
    cleanup()
  }

  after{
    cleanup()
  }

  test("basic file appender"){
    val testString = (1 to 1000).mkString(",")
    val inputStream = new ByteArrayInputStream(testString.getBytes(StandardCharsets.UTF_8))
    val appender = new FileAppender(inputStream,testFile)
    inputStream.close()
    appender.awaitTermination()
    assert(Files.toString(testFile,StandardCharsets.UTF_8) == testString)
  }

  def cleanup(): Unit ={
    testFile.delete()
  }
}
