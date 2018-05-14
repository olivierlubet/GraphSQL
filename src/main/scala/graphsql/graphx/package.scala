package graphsql

import org.apache.spark.graphx.Graph

package object graphx {
  type GraphSQL = Graph[Vertex, String]
}
