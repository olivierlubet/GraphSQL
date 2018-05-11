package graphsql.graphx

import java.net.URL
import java.io._


import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

object GraphWriter {
  def write(out: URL, graph: GraphSQL): Unit = {
    val file = new File(out.getPath)
    if (file.exists()) file.delete()
    file.createNewFile()

    val pw = new PrintWriter(file)
    write(pw, graph)
    pw.close()
  }

  def write(pw: PrintWriter, graph: GraphSQL): Unit = {
    pw.write("var data=" + pretty(render(json(graph))))//compact
  }

  def json(graph: GraphSQL): JValue = {

    val nodes = graph.vertices.map { case (id, name) =>
      ("id" -> id) ~ ("label" -> name)
    }.collect.toList


    val links = graph.edges.map { e =>
      ("from" -> e.srcId) ~ ("to" -> e.dstId)
    }.collect.toList

    ("nodes" -> nodes) ~ ("edges" -> links)
  }
}