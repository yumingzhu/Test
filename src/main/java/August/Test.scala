package August

import java.util.Date

import scala.util.Random

object Test {
  var date: Date = new Date()
  //  def  pf(string: String):PartialFunction[Int,Boolean]={
  //
  //  }


  val inc = new PartialFunction[Any, Int] {
    def apply(any: Any) = any.asInstanceOf[Int] + 1

    def isDefinedAt(any: Any) = if (any.isInstanceOf[Int]) true else false
  }

  def inc2: PartialFunction[Any, Int] = {
    case x: Int => x + 1
    case x:String =>x.hashCode
  }

  def main(args: Array[String]): Unit = {
    var x = null;
    //    println(Option("") )

//    List(1, 2, 3, "seven").collect { case i: Int => i + 1 }.foreach(println(_))
//    List(1, 2, 3, "seven").collect(inc).foreach(println(_))
//    List(1, 2, 3, "seven").collect(inc2).foreach(println(_))
    for (pid <- Random.shuffle(Seq.range(7, 10))) {
      println(pid)
    }


    //
    //    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    //    //    import spark.implicits._
    //    //    val properties = new Properties()
    //    //    val url = "jdbc:mysql://192.168.1.6:3306/gimc_tag?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false"
    //    //    properties.put("url", url)
    //    //    properties.put("user", "root")
    //    //    properties.put("password", "gnova2017!@#")
    //    //    properties.put("driver", "com.mysql.jdbc.Driver");
    //    //
    //    ////    spark.read.jdbc(url,"hbase_tag",properties).write.partitionBy("type","status").parquet("G:\\test\\hbase_tag")
    //    //    spark.read.parquet("G:\\test\\hbase_tag\\type=1").show()
    //    //    val s: String = "yk20190729:3,yk20190730:2,yk20190806:1"
    //    //    val value: String = "20190729"
    //    //    println("CDFAF2BF83BF79AB1688537A2316C910".toLowerCase)
    //    //    println("CDFAF2BF83BF79AB1688537A2316C910".length)
    //    //    val date = new Date()
    //    //   date.setTime(1566297622807L)
    //    //    println(date)
    //    //    spark.read.csv("file:///G:/test/xxxxxxxxxxxxxxxxxxxxxxxxxx").show(1000);
    //    //    spark.sql()
    //    //    for(n <- 1 to 10000 if n*n % (math.pow(10,n.toString.length)) == n )  println(n)
    //    //    println("xxxx")
    //    //    println("xxxx")
    //    //    var list=Array(1,2,3,4)
    //    //    println(Random.nextInt(list.size))
    //    val value =
    //    """{"data":{"boolean":"&&","split":true,"isLeaf":false,"children":[{"boolean":"&&","split":true,"isLeaf":false,"children":[{"boolean":"||","split":false,"isLeaf":false,"children":[{"tagId":"A0010","name":"20-25岁","number":56741103,"surviveTime":"6","frequency":"1","type":3,"isLeaf":true,"uid":4,"pid":3},{"tagId":"A0011","name":"26-30岁","number":123284652,"surviveTime":"6","frequency":"1","type":3,"isLeaf":true,"uid":5,"pid":3}],"uid":3,"pid":2},{"boolean":"||","split":false,"isLeaf":false,"children":[{"tagId":"B0066","name":"有车","number":140693535,"type":1,"isLeaf":true,"uid":7,"pid":6}],"pid":2,"uid":6}],"uid":2,"pid":1},{"boolean":"||","split":false,"isLeaf":false,"children":[{"tagId":"B0053","name":"自驾出行","number":363552,"type":1,"isLeaf":true,"uid":9,"pid":8},{"tagId":"B0054","name":"租车出行","number":676054,"type":1,"isLeaf":true,"uid":10,"pid":8}],"pid":1,"uid":8}],"uid":1}}"""
    //    var map = new mutable.HashMap[Int, ListBuffer[(String, String, String)]]()
    //    val jsonObject: JSONObject = JSON.parseObject(value).getJSONObject("data")
    //    var index = 1
    //    //    val sc: SparkContext = spark.sparkContext
    //    //    val accumulator = new TagAccumulator
    //    //    val accumulator2 = new TagAccumulator
    //    //    sc.register(accumulator)
    //    //    sc.register(accumulator2)
    //    //    sc.parallelize(0 to 100).map(x =>{  accumulator.add("1")}).collect()
    //    //    sc.parallelize(0 to 100).map(x =>{  accumulator2.add("1")}).collect()
    //    //    println(accumulator.value)
    //    //    println(accumulator2.value
    //    //存储不同层级的tagNode
    //    val mapTagNode = new mutable.HashMap[Int, ListBuffer[TagNode]]()
    //    val level = 1;
    //    println(Array(1, 2).headOption.get)


  }

  //  def parseJson(jsonObject: JSONObject, map: mutable.HashMap[Int, ListBuffer[TagNode]], index: Int): Unit = {
  //    val array: JSONArray = jsonObject.getJSONArray("children")
  //    if (array != null) {
  //      val opeart: String = jsonObject.getString("boolean")
  //      var tuple: (String, String, String) = null
  //      val one: JSONObject = array.getJSONObject(0)
  //      val tagIdOne: String = one.getString("tagId")
  //      val two: JSONObject = array.getJSONObject(1)
  //      val tagIdTwo: String = one.getString("tagId")
  //      if (two == null) {
  //
  //
  //      } else {
  //        (tagIdOne, tagIdTwo) match {
  //          case (null, null) => {
  //            parseJson(one, map, index + 1)
  //            new TagNode()
  //          }
  //          case (String, null) => {
  //            tuple = (opeart, tagIdOne, "1-1")
  //          }
  //          case (null, String) => {
  //            tuple = (opeart, "1-0", tagIdTwo)
  //          }
  //          case (String, String) => {
  //            tuple = (opeart, tagIdOne, tagIdTwo)
  //          }
  //        }
  //      }
  //    }
  //
  //  }


}
