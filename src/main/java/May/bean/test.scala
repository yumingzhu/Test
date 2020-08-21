package May.bean

import java.util.{Date, Properties}

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //    var sc = new SparkContext(conf)
    //    sc.textFile("G:/test/tagiii/*").map(x => {
    //      val date = new Date()
    //      (x, date.toString)
    //    });
    //  }.count()

    val map = new java.util.HashMap[String, String]
    map.put("url", "jdbc:mysql://192.168.1.6:3306/gimc_tag?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull")
    map.put("dbtable", "hbase_tag")
    map.put("user", "root")
    map.put("password", "gnova2017!@#")
    //    spark.read.format("jdbc").options(map).load().createTempView("hbase_tag");
    import spark.implicits._
    spark.read.format("jdbc").options(map).load().map(row => {
      val length: Int = row.getAs[String]("pids").split(",").length
      val tag_id: String = row.getAs[String]("tag_id")
      var str = "update  hbase_tag  set    `level`= " + length + "      where  tag_id=\"" + tag_id + "\";";
      str
    }).toJavaRDD.saveAsTextFile("G:\\test\\level");



    //    spark.sql("select tag_id from  hbase_tag where output_length=0").show();


    //    val frame = spark.sql("select   concat(location1,location2,location3) as location,split(value,',')[1]  as tag_id  from  temp  " +
    //      " left  join  local  where  split(temp.value,',')[0]=local.cn_city")
    //    val  properties =new Properties();
    //    properties.setProperty("user", "root");
    //    properties.setProperty("password", "gnova2017!@#")
    //    frame.write.mode(SaveMode.Overwrite).jdbc(
    //      "jdbc:mysql://192.168.1.6:3306/gimc_tag?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull",
    //      "location_tagid",
    //      properties );
    //    map.keySet().forEach(x=>{println(x)})]
    //    import  spark.implicits._
    //    spark.read.textFile("G:\\test\\city_code.txt").toDF().map(rowF).foreach(println(_));


  }

  def rowF(row: Row): String = {
    row.getString(0)
  }

}
