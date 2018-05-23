package graphsql.catalog

import graphsql._
import org.apache.spark.graphx.{Edge, VertexId}


class CatalogBrowser(catalog: NFCatalog) {
  lazy val vertices: Seq[Vertex] = {
    catalog.databases.values.flatMap(d => browse(d)).toSeq ++
      catalog.unreferencedTables.flatMap(d => browse(d)) ++
      catalog.unreferencedColumns.flatMap(d => browse(d))
  }

  lazy val edges: Seq[Edge[String]] = {
    vertices.flatMap {
      case d: NFDatabase => d.tables.values.map(t => Edge(d.id, t.id, "contains"))
      case t: NFTable => t.columns.values.map(c => Edge(t.id, c.id, "contains"))
      case c: NFColumn => c.usedFor.map(cc => Edge(c.id, cc.id, "used for"))
    }
  }

  val alreadySeen = scala.collection.mutable.HashMap.empty[VertexId, Vertex]

  def browse(vertex: Vertex): Seq[Vertex] = {
    if (alreadySeen contains vertex.id) {
      Seq.empty
    } else {
      alreadySeen += vertex.id -> vertex
      vertex match {
        case d: NFDatabase => vertex +: d.tables.values.flatMap(t => browse(t)).toSeq
        case t: NFTable => vertex +: t.columns.values.flatMap(c => browse(c)).toSeq
        case c: NFColumn => vertex +: c.usedFor.flatMap(cc => browse(cc))
      }
    }
  }
}
