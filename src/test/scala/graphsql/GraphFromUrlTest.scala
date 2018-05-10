package graphsql

import graphsql.controler.FileLoader
import org.scalatest.FunSuite

class GraphFromUrlTest extends FunSuite {
  test("test1.sql") {
    val g = GraphBuilder.buildFromURL(getClass.getResource("/test1.sql"))

    g.triplets.foreach{e =>
      println(s"$e.srcAttr $e.attr $e.dstAttr")
    }
  }
}
