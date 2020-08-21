package July

import java.net.URI
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.util.AccumulatorV2
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** *
  * 处理iqiyi 对应的日志信息
  */
object IqiyiLogMain {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val INPUTPATH = "hdfs://192.168.1.38:8020/data/log_data/media_log/iqiyi_log/";
  private val OUTPUTPATH = "hdfs://nameservice1/data/result_data/media_result/iqiyi/";

  private val CSV_FORMAT_NAME = "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("IqiyiLogMain")
      .master("local")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._

    //    sc.textFile(INPUTPATH + DateUtils.getTodayString).foreach(log => {
    var dateStr: String = "2019-07-26"
    val typeAccum = new TagAccumulator
    val channelAccum = new TagAccumulator
    sc.register(typeAccum, "typeAccum")
    sc.register(channelAccum, "channelAccum")
    sc.textFile("G:\\test\\iqiyi").foreach(log => {
      val str: String = getUseableMessage(log)
      var jsonObject: JSONObject = null;
      try {
        jsonObject = JSON.parseObject(str)
      } catch {
        case _ => {
          logger.info("json parse failure {}", log)
        }
      }
      add(typeAccum, channelAccum, jsonObject)
    })
    println(typeAccum.value)
    //        val typeMap: mutable.Map[String, Long] = typeAccum.value
    //        if (!typeMap.isEmpty) {
    //          sc.parallelize(typeMap.toList.sortBy(-_._2), 1).toDF("type", "number")
    //            .write.format(CSV_FORMAT_NAME).option("header", true)
    //            .mode(SaveMode.Overwrite).save(OUTPUTPATH + dateStr + "_type")
    //        }
    //        val channelMap: mutable.Map[String, Long] = channelAccum.value
    //        if (!channelMap.isEmpty) {
    //          sc.parallelize(channelMap.toList.sortBy(-_._2), 1).toDF("channelId", "number")
    //            .write.format(CSV_FORMAT_NAME).option("header", true)
    //            .mode(SaveMode.Overwrite).save(OUTPUTPATH + dateStr + "_channel")
    //        }

    sparkSession.stop()
    sc.stop()


  }


  private def add(typeAccum: TagAccumulator, channelAccum: TagAccumulator, jsonObject: JSONObject) = {
    if (jsonObject != null) {
      val device: JSONObject = jsonObject.getJSONObject("device")
      if (!device.isEmpty) {
        val platformId: String = device.getString("platform_id")
        if (StringUtils.isNoneBlank(platformId)) {
          typeAccum.add(platformId);
          typeAccum.add("SUMNEMBER")
        }
      }
      val site: JSONObject = jsonObject.getJSONObject("site")
      if (!site.isEmpty) {
        val content: JSONObject = site.getJSONObject("content")
        if (!content.isEmpty) {
          val channelId: String = content.getString("channel_id")
          if (StringUtils.isNoneBlank(channelId)) {
            channelAccum.add(channelId)
          }
        }
      }
    }
  }

  /**
    * 获取日志json部分
    *
    * @param message
    * @return
    */
  def getUseableMessage(message: String): String = {
    val begin: Int = message.indexOf("{")
    val end: Int = message.lastIndexOf("}")
    if (begin == -1 || end == -1) {
      return null
    }
    message.substring(begin, end + 1)
  }

  /**
    * Node 代替 字段在json中的层级关系
    *
    * @param nodeStr
    */
  class Node(nodeStr: String) extends Serializable {
    /**
      * 父级节点的关系 , true 代表为 数组， false 代表为对象
      */
    val parentNode = new util.ArrayList[(String, Boolean)]()

    nodeStr.split("->").foreach(str => {
      val strings: Array[String] = str.split("=>")
      if (strings.size == 2) {
        parentNode.add((strings(0), true))
        parentNode.add((strings(1), false))
      } else {
        parentNode.add((strings(0), false))
      }
    })
    val name: String = parentNode.remove(parentNode.size() - 1)._1
  }

  class NodeCompare extends Serializable {

    def NodeEquals(node: Node, eqValue: String): Unit = {
      //      if () {
      //
      //
      //      }


    }


  }


}
