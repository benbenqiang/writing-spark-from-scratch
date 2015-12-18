/**
 * Created by bbq on 2015/12/18
 */
object EnumerationDemo extends App{
  val a = TrafficLightColor.Red
  print(a)
}

object TrafficLightColor extends Enumeration{
  type TrafficLightColor = Value
  /**指定id和name*/
  val Red = Value(0,"stop")
  /**没有name则默认name为Yellow*/
  val Yellow = Value(10)
  /**没有指定id则从上一个递增*/
  val Green  = Value
}
