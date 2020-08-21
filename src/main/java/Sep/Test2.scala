package Sep

import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {


    //    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    //    val sc: SparkContext = spark.sparkContext
    //    import spark.implicits._
    //    spark.createDataset(List((1, "one", 100), (2, "one", 100), (1, "two", 99), (2, "two", 100))).toDF("id","xx","age").write.mode(SaveMode.Append)
    //    spark.createDataset(List((3, "one", 100), (4, "one", 100), (3, "two", 99), (4, "two", 100))).toDF("id","xx","age").write.mode(SaveMode.Append)
    //      .partitionBy("id").parquet("file:///G:/test/testparquet");

    //            spark.read.parquet(List("/data/insight/system/output/362"):_*).show(1000,false)
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.parallelize((1 to 20)).foreach(println(_))


    //    spark.sql()
    //    println(testadd(1)(2))


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
