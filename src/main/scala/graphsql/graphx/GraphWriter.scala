package graphsql.graphx

import java.net.URL
import java.io._

import org.json4s.JsonAST
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

object GraphWriter {
  def write(out: URL, graph: GraphSQL): Unit = {
    val pw = new PrintWriter(new File(out.getPath))
    write(pw, graph)
    pw.close()
  }

  def write(pw: PrintWriter, graph: GraphSQL): Unit = {
    pw.write(compact(render(json(graph))))
  }

  def json(graph: GraphSQL): JsonAST.JValue = {

    val nodes = graph.vertices.map { case (id, name) =>
      ("id" -> id) ~ ("name" -> name)
    }.collect.toList


    val links = graph.edges.map { e =>
      ("source" -> e.srcId) ~ ("target" -> e.dstId)
    }.collect.toList

    ("nodes" -> nodes) ~ ("links" -> links)
  }
}
