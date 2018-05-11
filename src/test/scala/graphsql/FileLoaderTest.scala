package graphsql

import java.net.URL

import graphsql.controler.FileLoader
import org.scalatest.FunSuite

import scala.io.Source

class FileLoaderTest extends FunSuite {

  test ("eraseComment") {
    assertResult("test")(FileLoader.eraseComment("test"))

    assertResult("test")(FileLoader.eraseComment("test-- comm"))

    assertResult("test")(FileLoader.eraseComment("test--\"rrr"))

    assertResult("test\"ti--ti")(FileLoader.eraseComment("test\"ti--ti"))
    assertResult("test'ti--ti")(FileLoader.eraseComment("test'ti--ti"))
    assertResult("test\"--\"")(FileLoader.eraseComment("test\"--\"--ti--ti"))
  }
  test("testAddFile") {
    val sqls = FileLoader.load(getClass.getResource("/test1.sql"))
    //println("sqls:")
    //sqls.foreach (println)
    assertResult(5)(sqls.length)
  }

}
