package August

/** *
  * lazy 只有真正的需要去使用这个变量的时候才会去  初始化变量
  *
  */
object TestScalaLazy {
  def init(): String = {
    println("初始化变量")
    return "ymz"
  }

  def main(args: Array[String]): Unit = {
    lazy val name = init()
    println("-----")
    println(name)
  }
}
