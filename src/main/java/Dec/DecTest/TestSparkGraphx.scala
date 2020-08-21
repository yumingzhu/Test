package Dec.DecTest

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestSparkGraphx {
  def main(args: Array[String]): Unit = {
     val conf =new SparkConf().setMaster("local").setAppName("spark graphx")
    val sc = new SparkContext(conf)
    val array=Array((3L,("rxin","student")),(7L,("jgonzal","postdoc")),(5L,("franklin","prof")),(2L,("istoica","prof")))
    val arrayEdge=Array(Edge(3L,7L,"cllab"),Edge(5L,3L,"advisor"),Edge(2L,5L,"colleague"),Edge(5L,7L,"pi"))

    val users: RDD[(VertexId, (String, String))] = sc.parallelize(array)
    val relationships: RDD[Edge[String]] = sc.parallelize(arrayEdge)
    val defaultUser=("john Doe","Missing")
    val graph=Graph(users,relationships,defaultUser)
    val postCOunt = graph.vertices.filter {
      case (id, (name, pos)) => pos == "postdoc"
    }.count()




    print(postCOunt)
    val l = graph.edges.filter(e=>e.srcId>e.dstId).count()
    print(l)
  }
}
