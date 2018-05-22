package graphsql

import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD


/*case class GraphSQL (VD) extends GraphImpl[Vertex, String](vertices, edges) {
  override def toString: String =
    vertices.sortBy(_._2.fullName).map { case (k, v: Vertex) => println(k + ":" + v.fullName) }.collect().mkString("\n") +
      edges.sortBy(_.srcId).collect.mkString("\n")
}*/

/*
abstract case class GraphSQL
(
  v: RDD[(VertexId, String)],
  e: RDD[Edge[String]]
)
  extends Graph[String, String](v, e) {

}
*/