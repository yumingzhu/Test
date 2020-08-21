package Oct

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object NetEaseLogMain {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.textFile("file:///F:/mobile.txt").map(x => x.split("\t").toList).filter(x => !x(2).equals("—"))
      .flatMap(x => {
        val tagId = x(0)
        val strings: List[String] = x(2).split(",").toList.map(
          x => {
            val brand: String = x.replaceAll("(\\s|-)+", "").toUpperCase
            val sql =
              "INSERT INTO `gimc_tag`.`device_type` (`brand`, `tag_id`, `create_date`, `update_date`, `del_flag`, `match_type`) VALUES" +
                "(\"" + brand + "\", \"" + tagId + "\", now(),  now(), 1, 1);"
            sql
          })
        strings
      }).foreach(println(_))


  }
}
