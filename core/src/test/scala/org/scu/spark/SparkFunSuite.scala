package org.scu.spark

import org.scalatest.{Outcome, BeforeAndAfterAll, FunSuite}

/**
 * 所有的测试用例都要继承这个类
 * Created by bbq on 2016/3/29
 */
private[spark] abstract class SparkFunSuite
  extends FunSuite
  with BeforeAndAfterAll
  with Logging
{
  //TODO afterAll

  final protected override def withFixture(test:NoArgTest) : Outcome={
    val testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("org.apapche.spark","o.a.s")
    try{
      logInfo(s"\n\n ====BBQ TEST OURPUT FOR $shortSuiteName : $testName =====\n")
      test()
    }finally {
      logInfo(s"\n\n ======BBQ FINISHED $shortSuiteName : $testName =====\n")
    }
  }

}
