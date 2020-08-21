package August

object TestSparkSqlDemo {
  def main(args: Array[String]): Unit = {
//    println(show(1))
    println(Set(1, 2, 3) -- Set(2, 4, 5))

  }

  @throws[Exception]
  def show(value: Int)(v: String) = {
    value + v
  }


}
