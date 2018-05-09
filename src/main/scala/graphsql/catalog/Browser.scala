package graphsql.catalog

import graphsql._
import org.apache.spark.graphx.{Edge, VertexId}


class Browser(catalog: Catalog) {
  lazy val vertices: Seq[Vertex] = {
    catalog.databases.values.flatMap(d => browse(d)).toSeq
  }
  lazy val edges: Seq[Edge[String]] = {
    vertices.flatMap {
      case d: Database => d.tables.values.map(t => Edge(d.id, t.id, "contains"))
      case t: Table => t.columns.values.map(c => Edge(t.id, c.id, "contains"))
      case c: Column => c.usedFor.map(cc => Edge(c.id, cc.id, "used for"))
    }
  }
  val alreadySeen = scala.collection.mutable.HashMap.empty[VertexId, Vertex]

  def browse(vertex: Vertex): Seq[Vertex] = {
    if (alreadySeen contains vertex.id) {
      Seq.empty
    } else {
      alreadySeen += vertex.id -> vertex
      vertex match {
        case d: Database => vertex +: d.tables.values.flatMap(t => browse(t)).toSeq
        case t: Table => vertex +: t.columns.values.flatMap(c => browse(c)).toSeq
        case c: Column => vertex +: c.usedFor.flatMap(cc => browse(cc))
      }
    }
  }
}
