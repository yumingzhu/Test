package June

import org.apache.spark.sql.SparkSession

object SparkSqlDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    spark.sql("SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x);").show()




  }

  private def add_months(spark: SparkSession) = {
    spark.sql("select add_months('2016-08-31',1)").show()
  }

}
