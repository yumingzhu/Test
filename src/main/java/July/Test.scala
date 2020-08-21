package July

import java.util.Properties
import java.util.regex.{Matcher, Pattern}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

object Test {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  case class Demo(a: Int)

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("Test").master("local").getOrCreate()
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._
    val accumulator: LongAccumulator = sc.longAccumulator
    iqiyiLog(sc)
    //
    import sparkSession.implicits._
    //    sc.textFile("G:/test/crowd/mac.txt")
    ////      .foreach(x=>println(getStrAppendMac(x)))
    //      .toDF("value").write.parquet("G:/test/device_idone")

    //      map(x => {
    //      val str: String = getUseableMessage(x)
    //      val id: String = JSON.parseObject(str).getJSONObject("device").getString("android_id")
    //      if (StringUtils.isNotBlank(id)) {
    //        id
    //      } else {
    //        null
    //      }
    //    }).filter(_ != null).saveAsTextFile("G:\\test\\device_Id")
    sc.stop()
    sparkSession.stop()
  }


  private def getIqiyiMac = {
    val sparkSession = SparkSession.builder().appName("TagAgeJob").master("local").getOrCreate()
    val sc = sparkSession.sparkContext
    val strings: List[String] = getBlackList(sparkSession)
    //    val blackList: Broadcast[List[String]] = sc.broadcast(strings)
    val accumulator: LongAccumulator = sc.longAccumulator
    sc.textFile("G:\\test\\a.log").
      map(x => {
        if (x.startsWith("J0007")) {
          x.split("\t")(1).substring(3);
        } else {
          null
        }
      }).filter(_ != null).distinct().foreach(println(_))
  }

  private def yicheLog(sc: SparkContext, blackList: Broadcast[List[String]]) = {
    val count: Long = sc.textFile("G:\\test\\yiche").map(x => {
      val str: String = getUseableMessage(x)
      val jsonObject: JSONObject = JSON.parseObject(str)
      var id: String = jsonObject.getJSONObject("user").getString("id")
      var macmd5: String = jsonObject.getJSONObject("device").getString("macmd5")
      if (StringUtils.isNotBlank(id) && blackList.value.contains(id)) {
        id = null
      }

      if (StringUtils.isNotBlank(macmd5) && blackList.value.contains(macmd5)) {
        macmd5 = null
      }

      if (id == null && StringUtils.isAnyBlank(macmd5)) {
        "1"
      } else {
        null
      }
    }).filter(_ != null).count()
    println(count)
  }

  private def iqiyiLog(sc: SparkContext) = {
    val count: Long = sc.textFile("G:\\test\\iqiyi.log").map(x => {
      val str: String = getUseableMessage(x)
      val id: String = JSON.parseObject(str).getJSONObject("user").getString("id")
      val platform_id: String = JSON.parseObject(str).getJSONObject("device").getString("platform_id")

      if (id.length == 32 && (platform_id .equals("33")||platform_id .equals("23"))) {
        id
      } else {
        null
      }
    }).filter(_ != null).count()
    println(count)
  }

  def containInsensitive(stringValue: String, regex: String): Boolean = {
    if (StringUtils.isBlank(stringValue) || StringUtils.isBlank(regex)) return false
    val pattern: Pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE)
    val matcher: Matcher = pattern.matcher(stringValue)
    matcher.find
  }

  def getUseableMessage(message: String): String = {
    val begin: Int = message.indexOf("{")
    val end: Int = message.lastIndexOf("}")
    if (begin == -1 || end == -1) {
      return null
    }
    message.substring(begin, end + 1)
  }

  def substrJson(log: String): String = {
    val i: Int = log.indexOf("\"7\"")
    if (i < 0) { //当前json 正确不需要解析
      return log
    }
    //截取,前面的数据
    val start: String = log.substring(0, i - 2)
    val substring: String = log.substring(i, log.length)
    val index: Int = substring.indexOf("}")
    val end: String = substring.substring(index, substring.length)
    val json: String = start + end
    json
  }

  def getBlackList(sparkSession: SparkSession): List[String] = {
    val properties = new Properties()
    val url = "jdbc:mysql://192.168.1.6:3306/gimc_tag?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false"
    properties.put("url", url)
    properties.put("user", "root")
    properties.put("password", "gnova2017!@#")
    properties.put("driver", "com.mysql.jdbc.Driver");
    val strings: Array[String] = sparkSession.read.jdbc(url, "black_list", properties).rdd.map(x => {
      val str: String = x.getAs[String]("value")
      str
    }).collect()
    strings.toList
  }

  /**
    * Mac 格式追加：000CE7C61462 and 00:0C:E7:C6:14:62
    *
    * @param mac
    * @return
    */
  def getStrAppendMac(mac: String): String = {
    val buffer = new StringBuffer
    if (!(mac.trim == "")) {
      val sub1: String = mac.substring(0, 2)
      val sub2: String = mac.substring(2, 4)
      val sub3: String = mac.substring(4, 6)
      val sub4: String = mac.substring(6, 8)
      val sub5: String = mac.substring(8, 10)
      val sub6: String = mac.substring(10, 12)
      buffer.append(sub1).append(":").append(sub2).append(":").append(sub3).append(":").append(sub4).append(":").append(sub5).append(":").append(sub6)
    }
    buffer.toString
  }

}
