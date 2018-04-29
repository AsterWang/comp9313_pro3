package comp9313.ass3

import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}

object Problem2 {
  def main(args: Array[String]): Unit = {
    // A graph with edge attributes containing distances
    val inputFile = args(0)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile =  sc.textFile(inputFile)
    val x = textFile.map(line => line.split(" "))
//    val x2 = x.map(t => Edge(t(1).toLong, t(2).toLong, t(3).toDouble))
    val x2 = x.map(t => (t(1).toLong, t(2).toLong)).distinct()
    //form the graph from the set of edges
    val graph = Graph.fromEdgeTuples(x2,1)
//    val graph = Graph.fromEdges(x2,1)
    val sourceId : VertexId = args(2).toInt
    val initialGraph = graph. mapVertices((id,_) => if (id.toString() == sourceId.toString) 0.0 else Double.PositiveInfinity)
    //在调用pregel方法时，initialGraph会被隐式转换成GraphOps类
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          //a user supplied function that is applied to outedges of vertices that received messages in the current
          //iteration
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      //最后Pregel对象的apply方法返回一个graph对象。
      // 用于聚合发送到同一个顶点的消息，这个函数的参数为两个A类型的消息，返回值为一个A类型的消息。
      //mergeMsg函数会对发送到同一节点的多个消息进行聚合，聚合的结果就是最小的那个值。
      (a,b) => math.min(a,b)
    )
    println(sssp.vertices.filter(t => t._2 != Double.PositiveInfinity).collect().mkString("\n"))
    println(sssp.vertices.filter(t => t._2 != Double.PositiveInfinity).count() - 1)
  }

}
