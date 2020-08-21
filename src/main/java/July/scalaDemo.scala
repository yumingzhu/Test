package July

import org.apache.hadoop.hbase.client.Delete

import collection.mutable._

object scalaDemo {
  def main(args: Array[String]): Unit = {
    //    val list = List(1 to 1000).map(_.toString().toInt)

    //    val map: Predef.Map[Int, Int] = list.map((_, 1)).toMap
    //    val pidMap: Predef.Map[Int, Int] = list.zipWithIndex.map(x=>(x._1,x._2+1)).toMap

    //    map.foreach(entry=>{
    //      val pid: Option[Int] = pidMap.get(entry._1)
    //      if(pid.isDefined) {
    //      }
    //    })
    println(List(
    3437495,
    2160954,
    578678,
    367490,
    341054,
    258983,
    144128

    ).sum-7288782)
    println("77D2E482A32095EA98FE9B48B5E532D7".length)
  }


  /** *
    * 方法前面加上类型， 再可以进行强制类型转化的时候进行转化，
    *
    * @param value
    * @tparam Double
    */
  def func[Double](value: Double): Unit = {
    println(value.isInstanceOf[Double])
    println(value)
  }

  def withScope[K](user: K): K = {
    user
  }

  //先执行add ，执行完成之后再执行，withScope
  def add(a: String): User = withScope {
    val user = new User("y", 1);
    user

  }


}

case class User(name: String, age: Int)
