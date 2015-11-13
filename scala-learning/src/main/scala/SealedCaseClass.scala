/**
 * 继承自sealed 类型的类在模式匹配中必须将全部可能都罗列出来
 * Created by applelab on 2015/11/13
 */
sealed trait Sex

case class Male() extends Sex

case class Female() extends Sex

object SealedCaseClass extends App {
  val people = Male().asInstanceOf[Sex]
  people match {
    //一下两个少一个就会Warning：match may not be exhaustive
    case Female() => println("female")
    case Male() => println("male")
  }
}
