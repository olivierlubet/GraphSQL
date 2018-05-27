package graphsql.graphx

import java.net.URL
import java.io._

import graphsql.Vertex
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

object GraphWriter {

  def write(file: File, graph: GraphSQL): Unit = write(file.toURI.toURL,graph)
  def write(out: URL, graph: GraphSQL): Unit = {
    val file = new File(out.getPath)
    if (file.exists()) file.delete()
    file.createNewFile()

    val pw = new PrintWriter(file)
    write(pw, graph)
    pw.close()
  }

  def write(pw: PrintWriter, graph: GraphSQL): Unit = {
    pw.write("var data=" + compact(render(json(graph)))) //compact | pretty
  }

  def json(graph: GraphSQL): JValue = {
println(graph.vertices.collect().size)// ici ano
    val nodes = graph.vertices.map { case (id, v)  => // name) =>
      ("id" -> id) ~ ("label" -> v.name) ~ ("group" -> v.group)
    }.collect.toList


    val links = graph.edges.map { e =>
      ("from" -> e.srcId) ~ ("to" -> e.dstId)
    }.collect.toList

    ("nodes" -> nodes) ~ ("edges" -> links)
  }
}
