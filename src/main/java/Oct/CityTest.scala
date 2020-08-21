package Oct

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object CityTest {
  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    val url = "jdbc:mysql://192.168.2.132:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false"
    properties.put("url", url)
    properties.put("user", "root")
    properties.put("password", "yumingzhu")
    properties.put("driver", "com.mysql.jdbc.Driver");

    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    val cityRdd: DataFrame = sc.textFile("file:///F://city.txt").map(x => {
      val arr: Array[String] = x.split("\t")
      (arr(0), arr(1))
    }).toDF("tagId", "cityName")
    cityRdd.createTempView("cityTag")

//    val cityRdd2: DataFrame = sc.textFile("file:///F://city2.txt").map(x => {
//      val arr: Array[String] = x.split("\t")
//      (arr(0), arr(1))
//    }).toDF("cityName", "tagIds")
//    cityRdd2.createTempView("cityTag2")

//    spark.sql("select  * from (select cityTag.* ,cityTag2.cityName as cname from  cityTag left join cityTag2 on  cityTag.cityName=cityTag2.cityName ) temp where cname is null ").show()

    spark.read.jdbc(url, "ip_region", properties).createTempView("ip_region")
//
    spark.sql("select location,tag_id from (select tagId as tag_id ,cityName,location from  cityTag  left join  ip_region on cityName=city) temp where location is  not null")
      .write.jdbc(url,"location_tagid",properties)

  }

}
