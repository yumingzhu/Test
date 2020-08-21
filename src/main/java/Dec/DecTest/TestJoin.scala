package Dec.DecTest

import org.apache.spark.sql.{SaveMode, SparkSession}

object TestJoin {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("ipdemo").master("local[*]").getOrCreate()

    import session.implicits._
    val list = List((1,"1"),(2,"2"),(3,"3"))
    val list1 = List((1,"a"),(2,"b"))
    val list2 = List((1,"A"),(2,"B"),(3,"C"))
    val df1 = session.createDataFrame(list)
    val df2 = session.createDataFrame(list1)
    val df3 = session.createDataFrame(list2)
    df1.toDF("num","work1").createTempView("df1")
    df2.toDF("num","work2").createTempView("df2")
    df3.toDF("num","work3").createTempView("df3")
    session.sql("select * from df1").show()
    val frame = session.sql("select  df1.num ,df1.work1,df2.work2 ,df3.work3 from df1  left outer join  df2  on  df1.num=df2.num   left outer join  df3  on  df1.num=df3.num ")
    frame.toDF()
    frame.write.mode(SaveMode.Append).csv("G:\\test\\dspdemo\\bidresult")

  }
}
