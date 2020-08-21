import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object Dec_Test_Spark_G1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("GraphXTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //构建点
    val vertexArray: Array[(Long, (String, Int))] = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    val edgeArray: Array[Edge[Int]] = Array(
      // 起点，终点， 边
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )
    //边的数据类型 ED:Int

    val vertexRDD = sc.parallelize(vertexArray)
    vertexRDD.repartition(1)
    val edgeRDD = sc.parallelize(edgeArray)
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    graph.vertices.filter {
      case (id, (name, age)) => age > 30
    }.collect.foreach(println)
    graph.edges.filter(e => e.attr > 5).collect.foreach(e => println(s"${e.srcId} to ${e.dstId}   att ${e.attr}"))
    println("找出图中 属性大于 5 的 tripltes:")
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect()) {
      println(s"${triplet.srcAttr._1}  likes  ${triplet.dstAttr._1}")
    }
    println("找出图中 最大 的出度， 入度， 度数")
    val maxV = graph.outDegrees.reduce((a, b) => if (a._2 > b._2) a else b)
    println(maxV)
    println("顶点的转换操作，顶点 age + 10：")
    graph.mapVertices { case (id, (name, age)) => (id, (name, age + 10)) }.vertices.collect().foreach(v => println(s"${v._2._1}  is  ${v._2._2}"));
    println("边的转换操作，边的属性*2：")
    graph.mapEdges(e => e.attr * 2).edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))

    println("结构操作")
    println("*" * 30)
    println("顶点年龄 》30 的子图")
    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 > 30)
    subGraph.vertices.collect().foreach(v => println(s"${v._2._1}  is   ${v._2._2}"))
    println("子图的所有顶点：")
    subGraph.edges.collect().foreach(e => println(s"${e.srcId}  to  ${e.dstId}  attr  ${e.attr}"))
    val inDegrees = graph.inDegrees
    case class User(name: String, age: Int, InDeg: Int, outDeg: Int)
    //创建一个新图， 顶点VD的数据 类型 为 User 并从 graph做类型转化
    val initiaUserGraph = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0) }
    val userGraph = initiaUserGraph.outerJoinVertices(initiaUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initiaUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.InDeg, outDegOpt.getOrElse(0))
    }
    userGraph.vertices.collect().foreach(v => println(s"${v._2.name} inDeg: ${v._2.InDeg} outDeg: ${v._2.outDeg}"))
    println("出度和入度 相同的人员：")
    userGraph.vertices.filter {
      case (id, u) => u.InDeg == u.outDeg
    }.collect().foreach(println)
    //聚合操作  ,
    println("找出年龄最大的follower:")
    val oldestFollower = userGraph.aggregateMessages[(String, Int)](et =>
      et.sendToDst(et.srcAttr.name, et.srcAttr.age),
      (a, b) => if (a._2 > b._2) a else b
    )
    userGraph.vertices.leftJoin(oldestFollower){
      (id, user, optoldestFollower) => optoldestFollower  match  {
        case  None =>  s"${user.name} does not have any followers."
        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      }
    }.collect().foreach{case (id ,str)=> println(str)}

    println("找出距离顶点最远的顶点，Pregel 基于对象")
    val  g=Pregel(graph.mapVertices((vid,vd)=>(0,vid)),(0,Long.MinValue),activeDirection =EdgeDirection.Out )(
      (id:VertexId,vd:(Int,Long),a:(Int,Long)) =>math.max(vd._1,a._1)  match {
        case vd._1 =>vd
        case a._1=>a
      },
      (et: EdgeTriplet[(Int, Long), Int]) => Iterator((et.dstId, (et.srcAttr._1 + 1+et.attr, et.srcAttr._2))) ,
      (a: (Int, Long), b: (Int, Long)) => math.max(a._1, b._1) match {
        case a._1 => a
        case b._1 => b
      })

    g.vertices.foreach(m=>println(s"原顶点${m._2._2}到目标顶点${m._1},最远经过${m._2._1}步"))

    // 面向对象
    val g2 = graph.mapVertices((vid, vd) => (0, vid)).pregel((0, Long.MinValue), activeDirection = EdgeDirection.Out)(
      (id: VertexId, vd: (Int, Long), a: (Int, Long)) => math.max(vd._1, a._1) match {
        case vd._1 => vd
        case a._1 => a
      },
      (et: EdgeTriplet[(Int, Long), Int]) => Iterator((et.dstId, (et.srcAttr._1 + 1, et.srcAttr._2))),
      (a: (Int, Long), b: (Int, Long)) => math.max(a._1, b._1) match {
        case a._1 => a
        case b._1 => b
      }
    )
    //   g2.vertices.foreach(m=>println(s"原顶点${m._2._2}到目标顶点${m._1},最远经过${m._2._1}步"))
    sc.stop()
  }
}
