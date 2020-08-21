package June

import java.util.{Date, Properties}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object SparkSqlDemo2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext
    val rdd1: RDD[(String, String)] = sc.textFile("G:\\test\\iqy.txt").map(x => {
      val strings: Array[String] = x.split(",")
      if (strings.length == 4) {
        (strings(0).toInt, strings(1), strings(3))
      } else {
        (2076, strings(1), "x")
      }

    }).filter(_._1 <= 272).map(x => (x._2, x._3))


    val properties = new Properties()
    val url = "jdbc:mysql://192.168.1.6:3306/gimc_tag?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false"
    properties.put("url", url)
    properties.put("user", "root")
    properties.put("password", "gnova2017!@#")
    properties.put("driver", "com.mysql.jdbc.Driver");
    val rdd2: RDD[(String, Int)] = spark.read.jdbc(url, "device_type", properties).rdd.map(row => {
      var types: String = row.getString(1)
      types = types.replaceAll(" ", "").replaceAll("\t", "").replaceAll("-", "").toUpperCase()
      (types, row.getInt(2))
    })
    val rdd3 = rdd1.join(rdd2)

    var list = List(
      "D0008" -> 1,
      "D0009" -> 2,
      "D0010" -> 3,
      "D0011" -> 4,
      "D0012" -> 5,
      "D0013" -> 6,
      "D0014" -> 7,
      "D0015" -> 9,
      "D0016" -> 10,
      "D0017" -> 11,
      "D0018" -> 8,
      "D0019" -> 12,
      "D0020" -> 13,
      "D0021" -> 14,
      "D0022" -> 15,
      "D0023" -> 16,
      "D0024" -> 17,
      "D0025" -> 18,
      "D0027" -> 19,
      "D0028" -> 20,
      "D0029" -> 33,
      "D0030"->21,
      "D0031" -> 22,
      "D0032" -> 23,
      "D0033" -> 30,
      "D0034" -> 24,
      "D0035" -> 25,
      "D0036" -> 26,
      "D0037" -> 27,
      "D0060" -> 31,
      "D0061" -> 32
    ).map(_.swap)
    val rdd4 = sc.parallelize(list)
    rdd3.map(x => {
      (x._2._2, x._1)
    }).join(rdd4).foreach(x => {

      var str = "INSERT  into  iqiyi_device(brand,tag_id,create_date,update_date,del_flag) VALUES(\"" + x._2._1 + "\",\"" + x._2._2 + "\",NOW(),NOW(),1);"
      println(str)
    })
  }

  private def getdeviceType = {
    val properties = new Properties()

    val url = "jdbc:mysql://192.168.1.6:3306/gimc_tag?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false"
    properties.put("url", url)
    properties.put("user", "root")
    properties.put("password", "gnova2017!@#")
    properties.put("driver", "com.mysql.jdbc.Driver");
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[String] = sc.textFile("G:\\test\\deviceType.txt")
    val deviceRdd: RDD[String] = rdd.filter(!_.equals("")).filter(x => {
      var flag = true;
      try {
        val int: Int = x.toInt
        flag = false;
      } catch {
        case _ =>
      }
      flag
    }).map(x => {
      var str = "INSERT  into  device_type(brand,type,create_date,updata_date,del_flag) VALUES(\"" + x + "\",17,NOW(),NOW(),1);"
      str
    })

    println(deviceRdd.count())
    deviceRdd.saveAsTextFile("G:\\test\\dfile");
  }
}
