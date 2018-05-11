package graphsql.graphx

import java.io.File

import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.json4s.JsonAST.{JField, JString, JValue}
import org.scalatest.FunSuite

class GraphWriterTest extends FunSuite {
  def fromSqlToJson(sql: String): JValue = {
    val g = GraphBuilder.buildFromSql(sql)
    GraphWriter.json(g)
  }

  def filter(j: JValue, value: String, field: String = "label"): List[(String, JValue)] = {
    j filterField {
      case JField("label", s: JString) => s.values == value
      case _ => false
    }
  }

  test("json") {
    val j = fromSqlToJson("select foo from baz")
    //println("render:"+pretty(render(j)))
    assertResult(1)(filter(j, "unknown.baz.foo").size)
  }

  test("write") {
    val fileURL = getClass.getResource("/data.js")
    //println("fileURL:"+fileURL.getPath)

    val g = GraphBuilder.buildFromURL(getClass.getResource("/test1.sql"))
    GraphWriter.write(fileURL, g)

    val file = new File(fileURL.getPath)
    assert(file.exists())
  }
}
