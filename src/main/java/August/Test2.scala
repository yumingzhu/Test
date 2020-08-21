package August

import com.google.common.base.Preconditions
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.driver.cores","2")
//    conf.set("spark.executor.cores","2")

//    conf.set("spark.testing.reservedMemory",200 * 1024 * 1024+"")
    conf.set("spark.memory.fraction","0.8")


    conf.setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO");
    val rdd1: RDD[Int] = sc.parallelize(1 to 1000)
    println(rdd1.take(10).toList)
    

//    val value: Int = rdd1.reduce(_ + _)
//    println(value)
    sc.stop()





  }

  def testadd(a: Int)(b: Int): Int = {
    return a + b;
  }

  //  def parseJson(jsonObject: JSONObject, map: mutable.HashMap[Int, ListBuffer[(String, String, String)]], index: Int): Unit = {
  //    val list = new ListBuffer[(String, String, String)]
  //    val array: JSONArray = jsonObject.getJSONArray("children")
  //    if (array != null) {
  //      val opeart: String = jsonObject.getString("boolean")
  //      var tuple: (String, String, String) = null
  //      val one: JSONObject = array.getJSONObject(0)
  //      val tagIdOne: String = one.getString("tagId")
  //      val two: JSONObject = array.getJSONObject(1)
  //      val tagIdTwo: String = one.getString("tagId")
  //      (tagIdOne, tagIdTwo) match {
  //        case (null, null) => {
  //          parseJson(one, map, index + 1)
  //          tuple = (opeart, "1-0", "1-1")
  //        }
  //        case (String, null) => {
  //          tuple = (opeart, tagIdOne, "1-1")
  //        }
  //        case (null, String) => {
  //          tuple = (opeart, "1-0", tagIdTwo)
  //        }
  //        case (String, String) => {
  //          tuple = (opeart, tagIdOne, tagIdTwo)
  //        }
  //      }
  //      list.+=(tuple)
  //    }
  //    map.put(index, list)
  //
  //  }


}
